"""`serena-shared`: drop-in replacement for the `serena` command in MCP client configs.

`serena-shared start-mcp-server --project-from-cwd [...]` attaches the client to one
shared streamable-http Serena server per (project root, server arguments), starting it
on demand. Any other invocation is handed to `serena` unchanged.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import IO, NoReturn

import anyio
import httpx2
from mcp.client.streamable_http import streamable_http_client
from mcp.server.stdio import stdio_server
from mcp.shared.inbound import MCP_PROTOCOL_VERSION_HEADER
from mcp.shared.message import ClientMessageMetadata, SessionMessage

from serena_shared.core import (
    HTTP_BASE,
    MAX_SOCKET_PATH,
    SERVE_UNIX,
    NotShareable,
    ServerRecord,
    ServerState,
    context_arg,
    find_project_root,
    is_shareable,
    pinned_context,
    serena_python,
    server_args,
    server_key,
    warn,
)

DEFAULT_IDLE_SECONDS = 1800
# How long a client waits, holding the lock, for a shared server to come up before it runs a
# private Serena instead. MCP clients give a server about 10 s (Codex) to 30 s (Claude Code)
# to start, and the private fallback needs time too. A slow server keeps starting in the
# background and later sessions use it.
START_WAIT_SECONDS = 6
EXIT_GRACE_SECONDS = 5


def _fail(message: str) -> NoReturn:
    warn(message)
    sys.exit(1)


def _serena_executable() -> str:
    configured = os.environ.get("SERENA_SHARED_SERENA") or "serena"
    found = shutil.which(configured)
    if not found:
        _fail(f"cannot find {configured!r}; install Serena or set SERENA_SHARED_SERENA")
    if os.path.realpath(found) == os.path.realpath(sys.argv[0]):
        _fail("`serena` resolves to serena-shared itself; set SERENA_SHARED_SERENA to the real Serena")
    return found


def _idle_seconds() -> int:
    raw = os.environ.get("SERENA_SHARED_IDLE_SECONDS", str(DEFAULT_IDLE_SECONDS))
    try:
        return max(0, int(raw))
    except ValueError:
        _fail(f"SERENA_SHARED_IDLE_SECONDS must be a whole number of seconds, not {raw!r}")


def _spawn_supervisor(state: ServerState, command: list[str], root: Path) -> None:
    state.rotate_log()
    state.write_json(
        state.spec_file,
        {"command": command, "root": str(root), "idle_seconds": _idle_seconds()},
    )
    with open(state.log_file, "ab") as log:
        # Returns once the supervisor holds its liveness lock and has forked itself away.
        subprocess.run(
            [sys.executable, "-m", "serena_shared.supervisor", str(state.dir)],
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            check=False,  # a supervisor that cannot start is detected by _wait_until_running
        )


def _wait_until_running(state: ServerState, deadline: float) -> ServerRecord | None:
    """Wait for the live supervisor to publish a healthy server; None once it has died."""
    while time.monotonic() < deadline:
        record = state.read_server()
        if state.healthy(record):
            return record
        if not state.supervisor_alive():
            return None
        time.sleep(0.2)
    return None


def _attach(state: ServerState, command: list[str], root: Path) -> tuple[str, IO[str]]:
    """Return the socket of a healthy server for this key, starting one if needed, plus this
    client's lease. Runs under `lock`, and takes the lease before releasing it, so the
    server cannot be reaped between attach and first request.

    Raises NotShareable when no shared server can be had; the caller then runs a private
    Serena rather than leaving the client without one."""
    # One deadline per client covers queueing for the lock, cleanup and waiting, so clients
    # arriving together never add their waits up.
    deadline = time.monotonic() + START_WAIT_SECONDS
    with state.locked(deadline):
        record = state.read_server()
        starting = record is None or record.get("state") == "starting"
        if not state.healthy(record) and starting and state.supervisor_alive():
            # Another client's start is still in progress (that client may have died waiting).
            record = _wait_until_running(state, deadline)
            if record is None and state.supervisor_alive():
                raise NotShareable(f"the shared server is still starting; see {state.log_file}")
        if not state.healthy(record):
            record = record or state.read_server()
            if record or state.server_alive():
                state.discard(record or {})
            _spawn_supervisor(state, command, root)
            record = _wait_until_running(state, deadline)
            if record is None:
                if not state.supervisor_alive():  # it died (e.g. SIGKILL): don't leave Serena behind
                    with contextlib.suppress(NotShareable):
                        state.discard(state.read_server() or {})
                raise NotShareable(f"the shared server did not start in time; see {state.log_file}")
        return record["socket"], state.take_lease()


def _exit_soon() -> None:
    """Safety net after a signal: stdin's reader thread cannot be cancelled, so a client that
    signals without closing stdin would otherwise keep this process (and its lease) alive."""
    timer = threading.Timer(EXIT_GRACE_SECONDS, os._exit, args=(0,))
    timer.daemon = True
    timer.start()


async def _relay(socket_path: str) -> None:
    # Tool calls can legitimately run longer than any read timeout.
    http = httpx2.AsyncClient(
        transport=httpx2.AsyncHTTPTransport(uds=socket_path),
        timeout=httpx2.Timeout(30.0, read=None),
    )
    protocol_version: str | None = None

    async def client_to_server(source, sink) -> None:
        async with sink:  # closing ends the HTTP session
            async for item in source:
                if isinstance(item, Exception):
                    warn(f"ignoring unreadable client message: {item}")
                    continue
                metadata = (
                    ClientMessageMetadata(headers={MCP_PROTOCOL_VERSION_HEADER: protocol_version})
                    if protocol_version
                    else None
                )
                await sink.send(SessionMessage(item.message, metadata=metadata))

    async def server_to_client(source, sink) -> None:
        nonlocal protocol_version
        async with sink:  # closing lets stdio_server finish and the process exit
            async for item in source:
                if isinstance(item, Exception):
                    raise item
                message = getattr(item.message, "root", item.message)
                result = getattr(message, "result", None)
                if isinstance(result, dict) and isinstance(result.get("protocolVersion"), str):
                    protocol_version = result["protocolVersion"]  # the initialize response
                await sink.send(SessionMessage(item.message))

    async with (
        http,
        stdio_server() as (client_read, client_write),
        streamable_http_client(f"{HTTP_BASE}/mcp", http_client=http) as (server_read, server_write),
        anyio.create_task_group() as tasks,
    ):

        async def run_then_stop(coro) -> None:
            await coro
            tasks.cancel_scope.cancel()

        async def stop_on_signal() -> None:
            with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGHUP, signal.SIGINT) as signals:
                async for _ in signals:
                    _exit_soon()
                    tasks.cancel_scope.cancel()  # still closes the HTTP session cleanly

        tasks.start_soon(run_then_stop, client_to_server(client_read, server_write))
        tasks.start_soon(run_then_stop, server_to_client(server_read, client_write))
        tasks.start_soon(stop_on_signal)


def _run_serena(serena: str, args: list[str], reason: str | None = None) -> NoReturn:
    if reason:
        warn(f"not sharing, running a private Serena: {reason}")
    os.execv(serena, [serena, *args])


def main() -> None:
    args = sys.argv[1:]
    serena = _serena_executable()
    root = find_project_root(Path.cwd()) if is_shareable(args) else None
    if root is None:
        _run_serena(serena, args)
    try:
        python = serena_python(serena)
        rewritten = server_args(args, root, pinned_context(python, context_arg(args)))
        state = ServerState.for_key(server_key(serena, rewritten))
        if len(os.fsencode(state.socket)) > MAX_SOCKET_PATH:
            raise NotShareable(f"socket path too long ({state.socket}); set a shorter SERENA_SHARED_STATE_DIR")
    except NotShareable as exc:
        _run_serena(serena, args, str(exc))

    command = [python, "-I", str(SERVE_UNIX), str(state.socket), *rewritten]
    try:
        socket_path, lease = _attach(state, command, root)
    except NotShareable as exc:
        _run_serena(serena, args, str(exc))
    with lease:
        anyio.run(_relay, socket_path)
