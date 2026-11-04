"""Share the Python HTTP relay between small native stdio clients."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

from serena_shared.core import ServerState, NotShareable


@contextlib.contextmanager
def startup_lock(directory: Path, timeout: float = 10):
    with (directory / 'broker-start.lock').open('a') as lock:
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise NotShareable('timed out waiting for relay broker')
                time.sleep(.05)
        yield


def connectable(path: Path) -> bool:
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(.2)
        try:
            client.connect(str(path))
            return True
        except OSError:
            return False


def launch_broker_client(state: ServerState, command: list[str], root: Path) -> None:
    """Start one broker per server key, then replace this Python client with nc."""
    from serena_shared.cli import START_WAIT_SECONDS
    path = state.dir / 'b.sock'
    with startup_lock(state.dir):
        if not connectable(path):
            state.write_json(state.dir / 'broker-spec.json', {
                'command': command, 'root': str(root), 'start_wait_seconds': START_WAIT_SECONDS,
            })
            with (state.dir / 'broker.log').open('ab') as log:
                process = subprocess.Popen(
                    [sys.executable, '-m', 'serena_shared.broker', str(state.dir)],
                    stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                    start_new_session=True,
                )
            deadline = time.monotonic() + 10
            while not connectable(path):
                if process.poll() is not None:
                    raise NotShareable(f'relay broker failed; see {state.dir / "broker.log"}')
                if time.monotonic() >= deadline:
                    process.terminate()
                    process.wait(timeout=5)
                    raise NotShareable('relay broker did not start in time')
                time.sleep(.05)
        (state.dir / 'broker-probe').touch()
    # Apple's system nc closes its write side at stdin EOF. Other nc variants have
    # incompatible EOF flags, so Linux uses the small standard-library proxy.
    if sys.platform == 'darwin' and Path('/usr/bin/nc').is_file():
        os.execv('/usr/bin/nc', ['nc', '-U', str(path)])
    os.execv(sys.executable, [sys.executable, '-S', str(Path(__file__).with_name('proxy.py')), str(path)])


async def serve(directory: Path) -> None:
    from serena_shared import cli
    from serena_shared.relay import client_session
    import anyio

    state = ServerState.for_key(directory.name)
    spec = json.loads((directory / 'broker-spec.json').read_text())
    cli.START_WAIT_SECONDS = spec.get('start_wait_seconds', cli.START_WAIT_SECONDS)
    active = 0
    last_activity = time.time()

    async def client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        nonlocal active, last_activity
        active += 1
        try:
            async def receive():
                return await reader.readline()

            async def send(data):
                writer.write(data.encode('utf-8'))
                await writer.drain()

            async def attach():
                return await anyio.to_thread.run_sync(
                    lambda: cli._attach(state, spec['command'], Path(spec['root'])))

            await client_session(receive, send, attach)
        except Exception as error:
            print(f'client relay failed: {type(error).__name__}: {error}', file=sys.stderr, flush=True)
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            active -= 1
            last_activity = time.time()

    path = directory / 'b.sock'
    with (directory / 'broker.lock').open('a') as lifetime:
        try:
            fcntl.flock(lifetime, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        path.unlink(missing_ok=True)
        server = await asyncio.start_unix_server(client, path=str(path), limit=16 * 1024 * 1024)
        os.chmod(path, 0o600)
        owns_path = True
        try:
            async with server:
                while True:
                    await asyncio.sleep(1)
                    if active == 0 and time.time() - last_activity >= 300:
                        # Startup probes are connections too and reset the grace period.
                        # Recheck under the same lock used by launching clients.
                        try:
                            with startup_lock(directory, timeout=0):
                                probe = (directory / 'broker-probe').stat().st_mtime
                                if active == 0 and time.time() - max(last_activity, probe) >= 300:
                                    server.close()
                                    path.unlink(missing_ok=True)
                                    owns_path = False
                                    fcntl.flock(lifetime, fcntl.LOCK_UN)
                                    return
                        except NotShareable:
                            continue
        finally:
            if owns_path:
                path.unlink(missing_ok=True)


if __name__ == '__main__':
    asyncio.run(serve(Path(sys.argv[1])))
