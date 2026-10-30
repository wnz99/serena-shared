"""Transparent JSON-RPC relay; Serena, rather than each proxy, validates MCP schemas."""

from __future__ import annotations

import contextlib
import json
import os
import signal
import sys
import threading
from collections.abc import AsyncIterator

import anyio
import httpx2

from serena_shared.core import HTTP_BASE, warn

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
    def __init__(self, http: httpx2.AsyncClient):
        self.http = http
        self.session_id: str | None = None
        self.protocol: str | None = None
        self.initialize_id = _NO_ID
        self.initialized = anyio.Event()
        self.notified = anyio.Event()
        self.notification_started = False
        self.writing = anyio.Lock()
        self.listening = False

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
            if isinstance(result, dict) and isinstance(result.get("protocolVersion"), str):
                self.protocol = result["protocolVersion"]
            self.initialized.set()
        async with self.writing:
            await anyio.to_thread.run_sync(self.write_line, data)
        return message

    @staticmethod
    def write_line(data: str) -> None:
        # Multiline SSE data is one newline-delimited stdio message.
        sys.stdout.write(json.dumps(json.loads(data), separators=(",", ":")) + "\n")
        sys.stdout.flush()

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
        request_id = message.get("id", _NO_ID)
        async with self.http.stream("POST", f"{HTTP_BASE}/mcp", json=message, headers=self.headers()) as response:
            response.raise_for_status()
            if response.headers.get("mcp-session-id"):
                self.session_id = response.headers["mcp-session-id"]
            if response.status_code == 202:
                if message.get("method") == "notifications/initialized" and not self.listening:
                    self.listening = True
                    self.notified.set()
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

    async def read_stdin(self, tasks: anyio.abc.TaskGroup) -> None:
        while line := await anyio.to_thread.run_sync(sys.stdin.buffer.readline, abandon_on_cancel=True):
            try:
                message = json.loads(line)
                if not isinstance(message, dict):
                    raise ValueError("expected an object")
            except (ValueError, UnicodeError):
                warn("ignoring unreadable client JSON-RPC message")
                continue
            if message.get("method") == "initialize":
                self.initialize_id = message.get("id", _NO_ID)
            elif self.initialize_id is not _NO_ID:
                await self.initialized.wait()
            if message.get("method") == "notifications/initialized":
                self.notification_started = True
            elif self.notification_started:
                await self.notified.wait()
            tasks.start_soon(self.post, message, tasks)
        tasks.cancel_scope.cancel()

    async def close(self) -> None:
        if self.session_id:
            with anyio.move_on_after(2, shield=True):
                with contextlib.suppress(httpx2.HTTPError):
                    await self.http.delete(f"{HTTP_BASE}/mcp", headers=self.headers())


async def relay(socket_path: str) -> None:
    async with httpx2.AsyncClient(
        transport=httpx2.AsyncHTTPTransport(uds=socket_path),
        timeout=httpx2.Timeout(30.0, read=None),
    ) as http:
        proxy = Relay(http)
        try:
            async with anyio.create_task_group() as tasks:
                async def stop_on_signal() -> None:
                    with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGHUP, signal.SIGINT) as signals:
                        async for _ in signals:
                            # A thread blocked on an open stdin cannot be cancelled at shutdown.
                            timer = threading.Timer(5, os._exit, args=(0,))
                            timer.daemon = True
                            timer.start()
                            tasks.cancel_scope.cancel()
                tasks.start_soon(proxy.read_stdin, tasks)
                tasks.start_soon(stop_on_signal)
        finally:
            await proxy.close()


def run_relay(socket_path: str) -> None:
    anyio.run(relay, socket_path)
