"""Real stdio proxies and Unix HTTP sockets exercise broker idle reconnection."""

import asyncio
import collections
import contextlib
import json
import sys
import tempfile
from pathlib import Path

import pytest

from serena_shared import broker, cli, proxy, relay
from serena_shared.core import ServerState


@pytest.mark.parametrize('native', [False, True])
def test_open_clients_release_leases_and_reconnect_without_replaying_tools(monkeypatch, native):
    if native and sys.platform != 'darwin':
        pytest.skip('native client uses the macOS system nc')

    async def scenario(directory):
        directory = Path(directory)
        monkeypatch.setenv('SERENA_SHARED_STATE_DIR', str(directory))
        state = ServerState.for_key('b' * 20)
        (state.dir / 'broker-spec.json').write_text(json.dumps({'command': [], 'root': str(directory)}))
        requests, leases, sessions = [], [], {}

        class Lease:
            closed = False
            def close(self):
                self.closed = True

        async def http(reader, writer):
            try:
                header = (await reader.readuntil(b'\r\n\r\n')).decode()
                lines = header.split('\r\n')
                method = lines[0].split()[0]
                headers = dict(line.lower().split(': ', 1) for line in lines[1:] if ': ' in line)
                content = await reader.readexactly(int(headers.get('content-length', 0)))
                session = headers.get('mcp-session-id')
                response_headers = ''
                if method == 'GET':
                    status, body = 405, b''
                elif method == 'DELETE':
                    requests.append(('DELETE', session))
                    status, body = 200, b''
                else:
                    message = json.loads(content)
                    name = message['method']
                    if name == 'initialize':
                        session = str(len(sessions) + 1)
                        sessions[session] = message['params']['clientInfo']['name']
                        response_headers = f'Mcp-Session-Id: {session}\r\n'
                        result = {'protocolVersion': '2025-03-26', 'capabilities': {},
                                  'serverInfo': {'name': 'fixture', 'version': '1'}}
                    else:
                        result = {'owner': sessions[session], 'arguments': message.get('params', {})}
                    requests.append((name, session))
                    status = 202 if name == 'notifications/initialized' else 200
                    body = b'' if status == 202 else json.dumps({
                        'jsonrpc': '2.0', 'id': message['id'], 'result': result,
                    }).encode()
                writer.write((f'HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\n'
                              f'Content-Length: {len(body)}\r\nConnection: close\r\n'
                              f'{response_headers}\r\n').encode() + body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        worker_socket = directory / 'worker.sock'
        worker = await asyncio.start_unix_server(http, str(worker_socket))

        def attach(*args):
            lease = Lease()
            leases.append(lease)
            return str(worker_socket), lease

        original = relay.client_session
        async def short_session(receive, send, attach):
            await original(receive, send, attach, idle_seconds=.04)

        monkeypatch.setattr(cli, '_attach', attach)
        monkeypatch.setattr(relay, 'client_session', short_session)
        broker_task = asyncio.create_task(broker.serve(state.dir))
        clients = []

        async def send(process, message):
            process.stdin.write(json.dumps(message).encode() + b'\n')
            await process.stdin.drain()

        async def response(process):
            return json.loads(await asyncio.wait_for(process.stdout.readline(), 3))

        async def wait_closed_leases(count):
            async with asyncio.timeout(3):
                while len(leases) < count or not all(lease.closed for lease in leases):
                    await asyncio.sleep(.01)

        try:
            async with asyncio.timeout(3):
                while not (state.dir / 'b.sock').exists():
                    await asyncio.sleep(.01)
            for number in range(2):
                command = ['/usr/bin/nc', '-U'] if native else [sys.executable, '-S', proxy.__file__]
                process = await asyncio.create_subprocess_exec(
                    *command, str(state.dir / 'b.sock'), stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                clients.append(process)
                await send(process, {'jsonrpc': '2.0', 'method': 'initialize', 'id': 1,
                                     'params': {'protocolVersion': '2025-03-26', 'capabilities': {},
                                                'clientInfo': {'name': f'client-{number}', 'version': '1'}}})
                assert (await response(process))['id'] == 1
                await send(process, {'jsonrpc': '2.0', 'method': 'notifications/initialized'})
            await wait_closed_leases(2)
            assert all(process.returncode is None for process in clients)
            for number, process in enumerate(clients):
                await send(process, {'jsonrpc': '2.0', 'method': 'tools/call', 'id': 2,
                                     'params': {'name': 'mutate', 'arguments': {'owner': number}}})
            results = await asyncio.gather(*(response(process) for process in clients))
            assert [result['id'] for result in results] == [2, 2]
            assert [result['result']['owner'] for result in results] == ['client-0', 'client-1']
            assert [result['result']['arguments']['arguments']['owner'] for result in results] == [0, 1]
            await wait_closed_leases(4)
            assert len(leases) == 4
            counts = collections.Counter(name for name, session in requests)
            assert counts == {'initialize': 4, 'notifications/initialized': 4, 'tools/call': 2, 'DELETE': 4}
            for process in clients:
                process.stdin.close()
                assert await asyncio.wait_for(process.wait(), 3) == 0
        finally:
            for process in clients:
                if process.returncode is None:
                    process.kill()
                await process.wait()
            broker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await broker_task
            worker.close()
            await worker.wait_closed()

    with tempfile.TemporaryDirectory(prefix='sbl-', dir='/tmp') as directory:
        asyncio.run(scenario(directory))
