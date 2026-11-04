"""Transparent JSON-RPC relay; Serena, rather than each proxy, validates MCP schemas."""

from __future__ import annotations

import contextlib
import json
import time
from collections.abc import AsyncIterator

import anyio
import httpx2

from serena_shared.core import HTTP_BASE

_NO_ID = object()


async def sse_messages(response: httpx2.Response) -> AsyncIterator[tuple[str, str | None]]:
    """Read complete SSE message events, including multiline data and resume IDs."""
    data: list[str] = []
    event = "message"
    event_id = None
    async for line in response.aiter_lines():
        if not line:
            if data and event == "message":
                yield "\n".join(data), event_id
            data, event, event_id = [], "message", None
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "data":
            data.append(value)
        elif field == "event":
            event = value
        elif field == "id" and "\0" not in value:
            event_id = value


class Relay:
    def __init__(self, http: httpx2.AsyncClient, send):
        self.http = http
        self.session_id: str | None = None
        self.protocol: str | None = None
        self.initialize_id = _NO_ID
        self.writing = anyio.Lock()
        self.listening = False
        self.send = send
        self.replaying = False
        self.server_requests = set()
        self.listen_scope = None
        self.initialize_result = None

    def headers(self, last_event_id: str | None = None) -> dict[str, str]:
        headers = {"Accept": "application/json, text/event-stream"}
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        if self.protocol:
            headers["MCP-Protocol-Version"] = self.protocol
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        return headers

    async def forward(self, data: str) -> dict:
        message = json.loads(data)
        if not isinstance(message, dict):
            raise ValueError("expected a JSON-RPC object from Serena")
        if message.get("id", _NO_ID) == self.initialize_id and "method" not in message:
            result = message.get("result")
            self.initialize_result = result
            if isinstance(result, dict) and isinstance(result.get("protocolVersion"), str):
                self.protocol = result["protocolVersion"]
            if self.replaying:
                if "error" in message:
                    raise RuntimeError("Serena rejected session reinitialization")
                return message
        if "method" in message and "id" in message:
            self.server_requests.add(message['id'])
        async with self.writing:
            await self.send(json.dumps(message, separators=(",", ":")) + "\n")
        return message

    async def read_response(self, response: httpx2.Response, request_id=_NO_ID) -> tuple[bool, str | None]:
        last_event_id = None
        async for data, event_id in sse_messages(response):
            if event_id is not None:
                last_event_id = event_id
            if not data:
                continue  # SSE priming events carry a resume ID without a JSON-RPC message.
            message = await self.forward(data)
            if request_id is not _NO_ID and message.get("id", _NO_ID) == request_id and "method" not in message:
                return True, last_event_id
        return False, last_event_id

    async def listen(self) -> None:
        with anyio.CancelScope() as scope:
            self.listen_scope = scope
            await self._listen()

    async def _listen(self) -> None:
        last_event_id = None
        # GET is safe to reconnect. POSTs are never replayed: they may execute tools.
        for attempt in range(4):
            try:
                async with self.http.stream("GET", f"{HTTP_BASE}/mcp", headers=self.headers(last_event_id)) as response:
                    if response.status_code == 405:
                        return  # an optional server-to-client stream
                    response.raise_for_status()
                    async for data, event_id in sse_messages(response):
                        if event_id is not None:
                            last_event_id = event_id
                        if not data:
                            continue
                        await self.forward(data)
            except httpx2.TransportError:
                if attempt == 3:
                    raise
            await anyio.sleep(0.2 * (attempt + 1))
        raise RuntimeError("Serena's notification stream closed repeatedly")

    async def post(self, message: dict, tasks: anyio.abc.TaskGroup) -> None:
        if 'method' not in message:
            self.server_requests.discard(message.get('id'))
        request_id = message.get("id", _NO_ID)
        async with self.http.stream("POST", f"{HTTP_BASE}/mcp", json=message, headers=self.headers()) as response:
            response.raise_for_status()
            if response.headers.get("mcp-session-id"):
                self.session_id = response.headers["mcp-session-id"]
            if response.status_code == 202:
                if message.get("method") == "notifications/initialized" and not self.listening:
                    self.listening = True
                    tasks.start_soon(self.listen)
                return
            if response.headers.get("content-type", "").split(";")[0].strip() == "application/json":
                await self.forward((await response.aread()).decode("utf-8"))
                return
            finished, event_id = await self.read_response(response, request_id)
        if request_id is _NO_ID or finished:
            return
        if not event_id:
            raise RuntimeError("Serena closed a request stream before its response")
        # Resume the interrupted SSE stream without executing the POST again.
        for _ in range(3):
            async with self.http.stream("GET", f"{HTTP_BASE}/mcp", headers=self.headers(event_id)) as response:
                response.raise_for_status()
                finished, resumed_id = await self.read_response(response, request_id)
                event_id = resumed_id or event_id
                if finished:
                    return
        raise RuntimeError("Serena's request stream could not be resumed")

    async def close(self) -> None:
        if self.session_id:
            with anyio.move_on_after(2, shield=True):
                with contextlib.suppress(httpx2.HTTPError):
                    await self.http.delete(f"{HTTP_BASE}/mcp", headers=self.headers())


class IdleSession:
    """A connected stdio client need not pin an unused HTTP session or worker lease."""

    def __init__(self, attach, tasks, send, idle_seconds):
        self.attach = attach
        self.tasks = tasks
        self.send = send
        self.idle_seconds = idle_seconds
        self.proxy = None
        self.http = None
        self.lease = None
        self.initialization = None
        self.ready = False
        self.active = 0
        self.touched = time.monotonic()
        self.lock = anyio.Lock()
        self.initialize_result = None
        self.stateful = False

    async def disconnect(self):
        if self.proxy is not None:
            if self.proxy.listen_scope is not None:
                self.proxy.listen_scope.cancel()
            await self.proxy.close()
        if self.http is not None:
            await self.http.aclose()
        if self.lease is not None:
            self.lease.close()
        self.proxy = self.http = self.lease = None

    async def connect(self):
        socket_path, self.lease = await self.attach()
        self.http = httpx2.AsyncClient(
            transport=httpx2.AsyncHTTPTransport(uds=socket_path),
            timeout=httpx2.Timeout(30.0, read=None),
        )
        self.proxy = Relay(self.http, self.send)
        if self.initialization is not None and self.ready:
            self.proxy.initialize_id = self.initialization['id']
            self.proxy.replaying = True
            await self.proxy.post(self.initialization, self.tasks)
            if self.proxy.initialize_result != self.initialize_result:
                raise RuntimeError('Serena capabilities changed while idle; restart the client')
            await self.proxy.post({'jsonrpc': '2.0', 'method': 'notifications/initialized'}, self.tasks)
            self.proxy.replaying = False

    async def post(self, message):
        async with self.lock:
            if self.proxy is None:
                try:
                    await self.connect()
                except Exception as error:
                    if 'id' in message and 'method' in message:
                        await self.send(json.dumps({
                            'jsonrpc': '2.0', 'id': message['id'],
                            'error': {'code': -32000, 'message': f'Cannot attach shared Serena: {error}'},
                        }) + '\n')
                    raise
            proxy = self.proxy
            if message.get('method') == 'initialize':
                self.initialization = message
                proxy.initialize_id = message.get('id', _NO_ID)
            self.active += 1
        try:
            await proxy.post(message, self.tasks)
            if message.get('method') == 'initialize':
                self.initialize_result = proxy.initialize_result
            if message.get('method') in ('resources/subscribe', 'logging/setLevel'):
                # These alter session state beyond the initialization handshake.
                # Preserve their session rather than silently losing that state.
                self.stateful = True
            if message.get('method') == 'notifications/initialized':
                self.ready = True
        finally:
            self.active -= 1
            self.touched = time.monotonic()

    async def reap(self):
        while True:
            await anyio.sleep(min(self.idle_seconds, 1))
            async with self.lock:
                if (self.ready and not self.stateful and self.proxy is not None and not self.active
                        and not self.proxy.server_requests
                        and time.monotonic() - self.touched >= self.idle_seconds):
                    await self.disconnect()


async def client_session(receive, send, attach, idle_seconds=300):
    """Broker entrypoint; I/O and worker ownership belong to the individual client."""
    session = None
    try:
        async with anyio.create_task_group() as tasks:
            session = IdleSession(attach, tasks, send, idle_seconds)
            tasks.start_soon(session.reap)
            # Read one message at a time through initialization. Once initialized,
            # POSTs run concurrently, including responses to server-side requests.
            while line := await receive():
                message = json.loads(line)
                if not isinstance(message, dict):
                    raise ValueError('expected a JSON-RPC object')
                if not session.ready:
                    await session.post(message)
                else:
                    tasks.start_soon(session.post, message)
            tasks.cancel_scope.cancel()
    finally:
        if session is not None:
            with anyio.CancelScope(shield=True):
                await session.disconnect()
