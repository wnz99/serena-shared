import asyncio
import contextlib
import json
import tempfile
from pathlib import Path

import pytest

from serena_shared import broker, relay
from serena_shared.core import NotShareable, ServerState


def test_startup_lock_is_exclusive_and_reaper_never_waits(tmp_path):
    with broker.startup_lock(tmp_path):
        with pytest.raises(NotShareable):
            with broker.startup_lock(tmp_path, timeout=0):
                pytest.fail('must not acquire another launcher lock')
    with broker.startup_lock(tmp_path, timeout=0):
        pass


def test_broker_keeps_client_streams_separate(monkeypatch):
    async def echo_session(receive, send, attach):
        while line := await receive():
            await send(line.decode())

    monkeypatch.setattr(relay, 'client_session', echo_session)
    with tempfile.TemporaryDirectory(dir='/tmp', prefix='sb-') as directory:
        monkeypatch.setenv('SERENA_SHARED_STATE_DIR', directory)
        state = ServerState.for_key('a' * 20)
        (state.dir / 'broker-spec.json').write_text(json.dumps({'command': [], 'root': directory}))

        async def scenario():
            task = asyncio.create_task(broker.serve(state.dir))
            path = state.dir / 'b.sock'
            try:
                for _ in range(100):
                    if path.exists():
                        break
                    await asyncio.sleep(.01)
                connections = [await asyncio.open_unix_connection(str(path)) for _ in range(2)]
                for i, (_, writer) in enumerate(connections):
                    writer.write(f'client-{i}\n'.encode())
                    await writer.drain()
                for i, (reader, writer) in enumerate(connections):
                    assert await asyncio.wait_for(reader.readline(), 2) == f'client-{i}\n'.encode()
                    writer.close()
                    await writer.wait_closed()
                await asyncio.sleep(.01)
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            assert not path.exists()

        asyncio.run(scenario())
