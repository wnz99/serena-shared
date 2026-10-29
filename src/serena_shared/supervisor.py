"""Detached owner of one shared Serena server.

Starts Serena on a Unix socket in the server's private state directory, publishes
`server.json` once the socket accepts connections, and stops Serena after no client lease
has been alive for `idle_seconds`.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from serena_shared.core import ServerState, kill_group, socket_ready

POLL_SECONDS = 5
START_TIMEOUT_SECONDS = 180
STOP_GRACE_SECONDS = 10


def _stop(child: subprocess.Popen) -> None:
    """Serena runs in its own process group; stop it together with its language servers."""
    if child.poll() is None:
        kill_group(child.pid, signal.SIGTERM)
        try:
            child.wait(STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            pass
    kill_group(child.pid, signal.SIGKILL)  # language servers that outlived Serena
    child.poll()


def _start(state: ServerState, spec: dict) -> subprocess.Popen | None:
    state.socket.unlink(missing_ok=True)  # left by a server that was killed
    with open(state.log_file, "ab") as log:
        child = subprocess.Popen(
            spec["command"],
            cwd=spec["root"],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
    deadline = time.monotonic() + START_TIMEOUT_SECONDS
    while child.poll() is None and time.monotonic() < deadline:
        if socket_ready(str(state.socket)) and child.poll() is None:
            return child
        time.sleep(0.2)
    _stop(child)
    return None


def run(state: ServerState) -> int:
    spec = json.loads(state.spec_file.read_text())
    me = os.getpid()
    child: subprocess.Popen | None = None

    def on_signal(signum, _frame):
        if child is not None:
            _stop(child)
        os._exit(128 + signum)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    # The client that started us holds the lock and is waiting on this record.
    state.write_json(state.server_file, {"state": "starting", "supervisor_pid": me})
    child = _start(state, spec)
    if child is None:
        return 1
    state.write_json(
        state.server_file,
        {"state": "running", "supervisor_pid": me, "serena_pid": child.pid,
         "socket": str(state.socket), "root": spec["root"]},
    )

    try:
        idle_seconds = spec["idle_seconds"]
        idle_since: float | None = None
        while child.poll() is None:
            time.sleep(POLL_SECONDS)
            if state.live_leases():
                idle_since = None
                continue
            idle_since = idle_since or time.monotonic()
            if time.monotonic() - idle_since < idle_seconds:
                continue
            with state.locked():  # a client attaching right now holds this lock
                if state.live_leases():
                    idle_since = None
                    continue
                state.remove_record_if_owned(me)
                _stop(child)
            return 0
        with state.locked():
            state.remove_record_if_owned(me)
        return 1
    finally:
        _stop(child)
        state.socket.unlink(missing_ok=True)


def main() -> None:
    state = ServerState(Path(sys.argv[1]))
    # Detach completely: the launching client reaps the intermediate process at once, so
    # this supervisor never lingers as a zombie that still looks alive to other clients.
    if os.fork():
        os._exit(0)
    sys.exit(run(state))


if __name__ == "__main__":
    main()
