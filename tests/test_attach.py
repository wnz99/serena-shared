"""Attach and cleanup paths, driven by a fake supervisor so they run in seconds."""

import json
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

from serena_shared import cli
from serena_shared.core import NotShareable, ServerState, is_locked

FAKE_SUPERVISOR = r"""
import fcntl, json, os, signal, sys, time
state_dir, mode, socket_path = sys.argv[1], sys.argv[2], sys.argv[3]
sup = open(os.path.join(state_dir, "supervisor.lock"), "a")
srv = open(os.path.join(state_dir, "server.lock"), "a")
fcntl.flock(sup, fcntl.LOCK_EX)
fcntl.flock(srv, fcntl.LOCK_EX)
if mode == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
record = {"supervisor_pid": os.getpid(), "serena_pid": 0, "socket": socket_path, "root": "/"}
def write(state):
    tmp = os.path.join(state_dir, ".server.json.tmp")
    with open(tmp, "w") as f:
        json.dump({"state": state, **record}, f)
    os.replace(tmp, os.path.join(state_dir, "server.json"))
if mode == "late-record":
    time.sleep(1)
    write("running")
elif mode in ("slow", "ignore-term"):
    write("starting" if mode == "slow" else "running")
time.sleep(60)
"""


@pytest.fixture
def state(monkeypatch):
    short = tempfile.mkdtemp(prefix="ssa-", dir="/tmp")  # Unix socket paths must stay short
    monkeypatch.setenv("SERENA_SHARED_STATE_DIR", short)
    yield ServerState.for_key("k")
    shutil.rmtree(short, ignore_errors=True)


@pytest.fixture
def listening_socket(state):
    path = str(state.dir / "live.sock")
    server = socket.socket(socket.AF_UNIX)
    server.bind(path)
    server.listen()
    yield path
    server.close()


@pytest.fixture
def fakes():
    procs = []

    def start(state, mode, socket_path="/nonexistent"):
        proc = subprocess.Popen([sys.executable, "-c", FAKE_SUPERVISOR, str(state.dir), mode, socket_path])
        procs.append(proc)
        deadline = time.monotonic() + 10
        while not is_locked(state.supervisor_lock) and time.monotonic() < deadline:
            time.sleep(0.02)
        return proc

    yield start
    for proc in procs:
        proc.kill()
        proc.wait()


def test_clients_arriving_together_never_add_up_their_waits(state, fakes, monkeypatch):
    monkeypatch.setattr(cli, "_spawn_supervisor", lambda s, *_: fakes(s, "slow"))
    results = []

    def client():
        started = time.monotonic()
        try:
            cli._attach(state, [], Path("/"))
            results.append(("attached", time.monotonic() - started))
        except NotShareable:
            results.append(("fallback", time.monotonic() - started))

    threads = [threading.Thread(target=client) for _ in range(3)]
    for thread in threads:
        thread.start()
        time.sleep(0.1)
    for thread in threads:
        thread.join()
    assert [kind for kind, _ in results] == ["fallback"] * 3
    assert max(elapsed for _, elapsed in results) < cli.START_WAIT_SECONDS + 1
    assert state.supervisor_alive(), "a slow server must keep starting for later sessions"


def test_waits_for_a_live_supervisor_that_has_not_written_its_record(state, fakes, listening_socket, monkeypatch):
    supervisor = fakes(state, "late-record", listening_socket)
    monkeypatch.setattr(cli, "_spawn_supervisor", lambda *_: pytest.fail("must not start a second server"))
    socket_path, lease = cli._attach(state, [], Path("/"))
    lease.close()
    assert socket_path == listening_socket
    assert supervisor.poll() is None


def test_supervisor_ignoring_sigterm_is_killed(state, fakes, listening_socket, monkeypatch):
    stuck = fakes(state, "ignore-term")  # running record whose socket is dead
    monkeypatch.setattr(cli, "_spawn_supervisor", lambda s, *_: fakes(s, "late-record", listening_socket))
    socket_path, lease = cli._attach(state, [], Path("/"))
    lease.close()
    assert socket_path == listening_socket
    assert stuck.wait(timeout=5) == -signal.SIGKILL
    assert json.loads(state.server_file.read_text())["supervisor_pid"] != stuck.pid
