"""`serena-shared`: drop-in replacement for the `serena` command in MCP client configs.

`serena-shared start-mcp-server --project-from-cwd [...]` attaches the client to one
shared streamable-http Serena server per (project root, server arguments), starting it
on demand. Any other invocation is handed to `serena` unchanged.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import IO, NoReturn


from serena_shared.core import (
    DISCARD_STEP_SECONDS,
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
# Total time a client spends getting a shared server (queueing for the lock, cleanup, waiting
# for startup) before it runs a private Serena instead. MCP clients give a server about 10 s (Codex) to 30 s (Claude Code)
# to start, and the private fallback needs time too. A slow server keeps starting in the
# background and later sessions use it.
START_WAIT_SECONDS = 6
DISCARD_BUDGET_SECONDS = 3 * DISCARD_STEP_SECONDS + 1  # discard's worst case plus a spawn


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
                if deadline - time.monotonic() < DISCARD_BUDGET_SECONDS:
                    # Too late to clean up within this client's startup budget; a later client
                    # with a fresh budget will.
                    raise NotShareable("a previous shared server is still being cleaned up")
                state.discard(record or {})
            _spawn_supervisor(state, command, root)
            record = _wait_until_running(state, deadline)
            if record is None:
                if not state.supervisor_alive():  # it died (e.g. SIGKILL): don't leave Serena behind
                    with contextlib.suppress(NotShareable):
                        state.discard(state.read_server() or {})
                raise NotShareable(f"the shared server did not start in time; see {state.log_file}")
        return record["socket"], state.take_lease()


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
        from serena_shared.relay import run_relay

        run_relay(socket_path)
