"""`serena-shared`: drop-in replacement for the `serena` command in MCP client configs.

`serena-shared start-mcp-server --project-from-cwd [...]` attaches the client to one
shared streamable-http Serena server per (project root, server arguments), starting it
on demand. Any other invocation is handed to `serena` unchanged.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import IO

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
    ServerState,
    context_arg,
    find_project_root,
    is_shareable,
    pid_alive,
    pinned_context,
    serena_python,
    server_args,
    server_key,
    warn,
)

DEFAULT_IDLE_SECONDS = 1800
ATTACH_TIMEOUT_SECONDS = 600  # covers START_ATTEMPTS slow starts


def _fail(message: str) -> None:
    warn(message)
    sys.exit(1)


def _serena_executable() -> str:
    found = os.environ.get("SERENA_SHARED_SERENA") or shutil.which("serena")
    if not found:
        _fail("cannot find `serena`; install it or set SERENA_SHARED_SERENA")
    if os.path.realpath(found) == os.path.realpath(sys.argv[0]):
        _fail("`serena` resolves to serena-shared itself; set SERENA_SHARED_SERENA to the real Serena")
    return found


def _spawn_supervisor(state: ServerState, command: list[str], root: Path) -> None:
    state.write_json(
        state.spec_file,
        {
            "command": command,
            "root": str(root),
            "idle_seconds": int(os.environ.get("SERENA_SHARED_IDLE_SECONDS", DEFAULT_IDLE_SECONDS)),
        },
    )
    with open(state.log_file, "ab") as log:
        subprocess.run(  # returns once the supervisor has forked itself away
            [sys.executable, "-m", "serena_shared.supervisor", str(state.dir)],
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            check=True,
        )


def _attach(state: ServerState, command: list[str], root: Path) -> tuple[str, IO[str]]:
    """Return the socket of a healthy server for this key, starting one if needed, plus this
    client's lease. The lease is taken while still holding the lock, so the server cannot be
    reaped between attach and first request."""
    deadline = time.monotonic() + ATTACH_TIMEOUT_SECONDS
    with state.locked():
        spawned = False
        while True:
            record = state.read_server()
            if state.healthy(record):
                return record["socket"], state.take_lease()
            starting = record and record.get("state") == "starting" and pid_alive(record.get("supervisor_pid", 0))
            if starting and time.monotonic() < deadline:
                time.sleep(0.2)  # a supervisor (ours or one whose client died) is still starting
                continue
            if record:
                state.discard(record)
            if spawned:
                _fail(f"shared Serena server failed to start; see {state.log_file}")
            _spawn_supervisor(state, command, root)
            spawned = True
            while state.read_server() is None and time.monotonic() < deadline:
                time.sleep(0.05)


async def _relay(socket_path: str) -> None:
    # Tool calls can legitimately run longer than any read timeout.
    http = httpx2.AsyncClient(
        transport=httpx2.AsyncHTTPTransport(uds=socket_path),
        timeout=httpx2.Timeout(30.0, read=None),
    )
    protocol_version: str | None = None

    async def client_to_server(source, sink) -> None:
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
            with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGHUP) as signals:
                async for _ in signals:
                    tasks.cancel_scope.cancel()  # still closes the HTTP session cleanly

        tasks.start_soon(run_then_stop, client_to_server(client_read, server_write))
        tasks.start_soon(run_then_stop, server_to_client(server_read, client_write))
        tasks.start_soon(stop_on_signal)


def main() -> None:
    args = sys.argv[1:]
    serena = _serena_executable()
    root = find_project_root(Path.cwd()) if is_shareable(args) else None
    python = serena_python(serena) if root else None
    context_file = pinned_context(python, context_arg(args)) if python else None
    if root is None or python is None or context_file is None:
        os.execv(serena, [serena, *args])

    rewritten = server_args(args, root, context_file)
    state = ServerState.for_key(server_key(serena, rewritten))
    if len(str(state.socket)) > MAX_SOCKET_PATH:
        warn(f"not sharing: socket path too long ({state.socket}); set a shorter SERENA_SHARED_STATE_DIR")
        os.execv(serena, [serena, *args])
    command = [python, "-I", str(SERVE_UNIX), str(state.socket), *rewritten]
    socket_path, lease = _attach(state, command, root)
    with lease:
        anyio.run(_relay, socket_path)
