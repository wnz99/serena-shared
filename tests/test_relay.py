"""Exercise the HTTP/stdin boundary without importing MCP's model graph into the proxy."""

import io
import json
import subprocess
import sys

import anyio
import httpx2

from serena_shared.relay import Relay, sse_messages


def test_cli_and_fallback_import_without_the_mcp_sdk():
    result = subprocess.run([sys.executable, "-c", """
import importlib.abc, sys
class NoSDK(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname in ('mcp', 'mcp_types', 'pydantic'):
            raise AssertionError('proxy imported protocol schemas: ' + fullname)
sys.meta_path.insert(0, NoSDK())
import serena_shared.cli
import serena_shared.relay
"""], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_sse_multiline_comments_and_event_ids():
    async def scenario():
        response = httpx2.Response(200, content=b': keepalive\r\nid: 7\r\nevent: message\r\ndata: {"id":1,\r\ndata: "result":{}}\r\n\r\nevent: ping\ndata: ignore\n\n')
        return [item async for item in sse_messages(response)]
    assert anyio.run(scenario) == [('{"id":1,\n"result":{}}', "7")]


def test_negotiated_headers_json_responses_and_session_cleanup(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdout", output)
    requests = []

    def respond(request):
        requests.append(request)
        if request.method == "DELETE":
            return httpx2.Response(200)
        body = json.loads(request.content)
        if body["method"] == "initialize":
            return httpx2.Response(200, headers={"Mcp-Session-Id": "session"}, json={
                "jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2025-03-26"},
            })
        return httpx2.Response(200, json={"jsonrpc": "2.0", "id": 2, "result": {"tools": []}})

    async def scenario():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
            proxy = Relay(http)
            proxy.initialize_id = 1
            async with anyio.create_task_group() as tasks:
                await proxy.post({"jsonrpc": "2.0", "id": 1, "method": "initialize"}, tasks)
                await proxy.post({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, tasks)
            await proxy.close()

    anyio.run(scenario)
    assert [json.loads(line)["id"] for line in output.getvalue().splitlines()] == [1, 2]
    assert requests[1].headers["Mcp-Session-Id"] == "session"
    assert requests[1].headers["MCP-Protocol-Version"] == "2025-03-26"
    assert requests[-1].method == "DELETE"


def test_interrupted_tool_stream_resumes_without_replaying_post(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdout", output)
    methods = []

    def respond(request):
        methods.append(request.method)
        if request.method == "POST":
            return httpx2.Response(200, headers={"Content-Type": "text/event-stream"}, content=(
                'id: progress-1\ndata:\n\n'
                'data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}}\n\n'
            ))
        assert request.headers["Last-Event-ID"] == "progress-1"
        return httpx2.Response(200, headers={"Content-Type": "text/event-stream"}, content=(
            'data: {"jsonrpc":"2.0","id":3,"result":{"content":[]}}\n\n'
        ))

    async def scenario():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
            async with anyio.create_task_group() as tasks:
                await Relay(http).post({"jsonrpc": "2.0", "id": 3, "method": "tools/call"}, tasks)

    anyio.run(scenario)
    assert methods == ["POST", "GET"]
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert messages[0]["method"] == "notifications/progress"
    assert messages[1]["id"] == 3


def test_notification_stream_preserves_empty_priming_resume_id(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdout", output)
    calls = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            body = 'id: prime-1\ndata:\n\n'
        elif len(calls) == 2:
            assert request.headers["Last-Event-ID"] == "prime-1"
            body = 'id: next-2\ndata: {"jsonrpc":"2.0","method":"notifications/tools/list_changed"}\n\n'
        else:
            return httpx2.Response(405)
        return httpx2.Response(200, headers={"Content-Type": "text/event-stream"}, content=body)

    async def scenario():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
            await Relay(http).listen()

    anyio.run(scenario)
    assert len(output.getvalue().splitlines()) == 1
    assert json.loads(output.getvalue())["method"] == "notifications/tools/list_changed"
