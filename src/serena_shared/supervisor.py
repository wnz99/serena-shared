"""Detached owner of one shared Serena server.

Starts Serena on a Unix socket in the server's private state directory, publishes
`server.json` once the socket accepts connections, and stops Serena after no client lease
has been alive for `idle_seconds`.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import IO

from serena_shared.core import STOP_GRACE_SECONDS, ServerState, kill_group, socket_ready

POLL_SECONDS = 5
START_TIMEOUT_SECONDS = 180


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


def _launch(state: ServerState, spec: dict, server_lock_fd: int) -> subprocess.Popen:
    state.socket.unlink(missing_ok=True)  # left by a server that was killed
    with open(state.log_file, "ab") as log:
        return subprocess.Popen(
            spec["command"],
            cwd=spec["root"],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            pass_fds=(server_lock_fd,),  # Serena keeps server.lock held if we die first
            # run() blocks stop signals around this call; Serena must not inherit that mask.
            preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, set()),  # noqa: PLW1509 - no threads here
        )


def _wait_ready(state: ServerState, child: subprocess.Popen) -> bool:
    deadline = time.monotonic() + START_TIMEOUT_SECONDS
    while child.poll() is None and time.monotonic() < deadline:
        if socket_ready(str(state.socket)) and child.poll() is None:
            return True
        time.sleep(0.2)
    return False


def run(state: ServerState, server_lock: IO[str]) -> int:
    spec = json.loads(state.spec_file.read_text())
    me = os.getpid()
    child: subprocess.Popen | None = None

    def on_signal(signum, _frame):
        if child is not None:
            _stop(child)
        # Safe without `lock`: no other supervisor can start while we hold our locks.
        state.remove_record_if_owned(me)
        state.socket.unlink(missing_ok=True)
        os._exit(128 + signum)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    # The client that started us holds `lock` and is waiting on this record. Serena's PID is
    # in it from the start, so a client can clean up even if we die during startup.
    stop_signals = {signal.SIGTERM, signal.SIGINT}
    signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)  # no signal between launch and assignment
    try:
        child = _launch(state, spec, server_lock.fileno())
    finally:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
    record = {"supervisor_pid": me, "serena_pid": child.pid, "socket": str(state.socket), "root": spec["root"]}
    state.write_json(state.server_file, {"state": "starting", **record})
    if not _wait_ready(state, child):
        _stop(child)
        state.remove_record_if_owned(me)
        return 1
    state.write_json(state.server_file, {"state": "running", **record})

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
                _stop(child)  # before removing the record: its PIDs are needed if we die midway
                state.remove_record_if_owned(me)
            return 0
        with state.locked():
            state.remove_record_if_owned(me)
        return 1
    finally:
        _stop(child)
        state.socket.unlink(missing_ok=True)


def main() -> None:
    state = ServerState(Path(sys.argv[1]))
    # Take both locks before detaching: the forked child shares the locked files, so they are
    # already held when the launching client sees this process exit.
    supervisor_lock = open(state.supervisor_lock, "a")  # noqa: SIM115 - held for the process lifetime
    server_lock = open(state.server_lock, "a")  # noqa: SIM115 - held for the process lifetime
    try:
        fcntl.flock(supervisor_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(server_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(1)  # processes of a previous server for this key are still alive
    # Detach completely: the launching client reaps this intermediate process at once, so
    # the supervisor never lingers as a zombie under it.
    if os.fork():
        os._exit(0)
    sys.exit(run(state, server_lock))


if __name__ == "__main__":
    main()
