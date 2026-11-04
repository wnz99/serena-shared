import json

import anyio
import httpx2

from serena_shared.relay import IdleSession


def test_idle_disconnect_releases_lease_and_replays_only_handshake(monkeypatch):
    requests, output, leases = [], [], []

    class Lease:
        closed = False
        def close(self):
            self.closed = True

    async def respond(request):
        if request.method == 'GET':
            return httpx2.Response(405)
        if request.method == 'DELETE':
            requests.append('DELETE')
            return httpx2.Response(200)
        body = json.loads(request.content)
        requests.append(body['method'])
        if body['method'] == 'notifications/initialized':
            return httpx2.Response(202)
        return httpx2.Response(200, headers={'Mcp-Session-Id': 'session'}, json={
            'jsonrpc': '2.0', 'id': body['id'],
            'result': {'protocolVersion': '2025-03-26'} if body['method'] == 'initialize' else {},
        })

    monkeypatch.setattr(httpx2, 'AsyncHTTPTransport', lambda **kwargs: httpx2.MockTransport(respond))

    async def scenario():
        async def attach():
            lease = Lease()
            leases.append(lease)
            return '/unused', lease

        async def send(data):
            output.append(json.loads(data))

        async with anyio.create_task_group() as tasks:
            session = IdleSession(attach, tasks, send, .02)
            tasks.start_soon(session.reap)
            await session.post({'jsonrpc': '2.0', 'method': 'initialize', 'id': 1})
            await session.post({'jsonrpc': '2.0', 'method': 'notifications/initialized'})
            with anyio.fail_after(2):
                while not leases[0].closed:
                    await anyio.sleep(.01)
            await session.post({'jsonrpc': '2.0', 'method': 'tools/call', 'id': 2})
            await session.disconnect()
            tasks.cancel_scope.cancel()

    anyio.run(scenario)
    assert len(leases) == 2 and all(lease.closed for lease in leases)
    assert [item['id'] for item in output] == [1, 2]
    assert requests == ['initialize', 'notifications/initialized', 'DELETE',
                        'initialize', 'notifications/initialized', 'tools/call', 'DELETE']


def test_idle_reaper_preserves_inflight_and_server_requests_and_state(monkeypatch):
    async def scenario():
        class Lease:
            closed = False
            def close(self): self.closed = True
        class Proxy:
            server_requests = set()
            listen_scope = None
            async def close(self): pass
        async with anyio.create_task_group() as tasks:
            session = IdleSession(None, tasks, None, .01)
            session.proxy, session.lease, session.ready = Proxy(), Lease(), True
            session.touched = 0
            tasks.start_soon(session.reap)
            session.active = 1
            await anyio.sleep(.03)
            assert not session.lease.closed
            session.active = 0
            session.proxy.server_requests.add('server-question')
            await anyio.sleep(.03)
            assert not session.lease.closed
            session.proxy.server_requests.clear()
            session.stateful = True
            await anyio.sleep(.03)
            assert not session.lease.closed
            session.stateful = False
            lease = session.lease
            with anyio.fail_after(1):
                while not lease.closed:
                    await anyio.sleep(.01)
            tasks.cancel_scope.cancel()
    anyio.run(scenario)
