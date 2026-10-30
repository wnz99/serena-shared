"""End-to-end: real Serena, real MCP clients. Skipped when `serena` is not installed."""

import contextlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import anyio
import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

pytestmark = pytest.mark.skipif(shutil.which("serena") is None, reason="serena not installed")

SHIM = str(Path(sys.executable).with_name("serena-shared"))
# Shared servers always run without a dashboard; these flags matter for the private-Serena
# fallback, which would otherwise open a browser tab with a fresh SERENA_HOME's defaults.
NO_DASHBOARD = ("--enable-web-dashboard", "false", "--open-web-dashboard", "false")


@pytest.fixture(autouse=True)
def cold_backend_budget(tmp_path, monkeypatch):
    # Protocol/sharing tests must wait for cold language servers on loaded machines.
    # test_attach.py separately checks the production six-second fallback budget.
    launcher = tmp_path / "serena-shared"
    launcher.write_text(
        f"#!{sys.executable}\n"
        "from serena_shared import cli\n"
        "cli.START_WAIT_SECONDS = 180\n"
        "cli.main()\n"
    )
    launcher.chmod(0o700)
    monkeypatch.setattr(sys.modules[__name__], "SHIM", str(launcher))


@pytest.fixture
def repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".serena").mkdir()
    (repo / ".serena" / "project.yml").write_text("project_name: fixture\nlanguages: [python]\n")
    (repo / "app.py").write_text("def greet(name):\n    return f'hi {name}'\n")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    return repo


@pytest.fixture
def env(tmp_path):
    env = dict(os.environ)
    # pytest's tmp_path is too long for a Unix socket path
    state = tempfile.mkdtemp(prefix="ss-", dir="/tmp")
    env["SERENA_SHARED_STATE_DIR"] = state
    env["SERENA_SHARED_IDLE_SECONDS"] = "3"
    env["SERENA_HOME"] = str(tmp_path / "serena-home")  # keep test projects out of the user's config
    yield env
    shutil.rmtree(state, ignore_errors=True)


def servers(env) -> list[dict]:
    root = Path(env["SERENA_SHARED_STATE_DIR"])
    records = [json.loads(p.read_text()) for p in root.glob("*/server.json")]
    return [r for r in records if r.get("state") == "running"]


async def use_serena(repo, env, context, hold: anyio.Event | None = None) -> set[str]:
    params = StdioServerParameters(
        command=SHIM,
        args=["start-mcp-server", f"--context={context}", "--project-from-cwd", "--agent-interface", "tools", *NO_DASHBOARD],
        cwd=str(repo),
        env=env,
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = {tool.name for tool in (await session.list_tools()).tools}
        result = await session.call_tool("get_symbols_overview", {"relative_path": "app.py"})
        assert "greet" in json.dumps(result.model_dump()), result
        if hold is not None:
            await hold.wait()
        return tools


def test_concurrent_clients_share_one_server_until_idle(repo, env):
    async def scenario():
        release = anyio.Event()
        results = []
        async with anyio.create_task_group() as tg:
            async def client():
                results.append(await use_serena(repo, env, "codex", release))
            tg.start_soon(client)
            tg.start_soon(client)
            with anyio.fail_after(240):
                while len(servers(env)) == 0 or len(list(Path(env["SERENA_SHARED_STATE_DIR"]).glob("*/leases/*"))) < 2:
                    await anyio.sleep(0.5)
            assert len(servers(env)) == 1
            release.set()
        return results

    results = anyio.run(scenario)
    assert all("activate_project" not in tools for tools in results)
    assert all("find_symbol" in tools for tools in results)

    serena_pid = servers(env)[0]["serena_pid"]
    deadline = time.monotonic() + 60
    while servers(env) and time.monotonic() < deadline:
        time.sleep(1)
    assert servers(env) == []
    time.sleep(1)
    assert subprocess.run(["ps", "-p", str(serena_pid)], capture_output=True, check=False).returncode != 0


def test_contexts_get_separate_servers(repo, env):
    async def scenario():
        release = anyio.Event()
        async with anyio.create_task_group() as tg:
            tg.start_soon(use_serena, repo, env, "codex", release)
            tg.start_soon(use_serena, repo, env, "claude-code", release)
            with anyio.fail_after(240):
                while len(servers(env)) < 2:
                    await anyio.sleep(0.5)
            release.set()

    anyio.run(scenario)
    deadline = time.monotonic() + 60
    while servers(env) and time.monotonic() < deadline:
        time.sleep(1)
    assert servers(env) == []


def test_shared_server_cannot_switch_projects(repo, env):
    async def scenario():
        params = StdioServerParameters(
            command=SHIM,
            args=["start-mcp-server", "--context=codex", "--project-from-cwd", "--agent-interface", "tools", *NO_DASHBOARD],
            cwd=str(repo),
            env=env,
        )
        async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
            await session.initialize()
            tools = {tool.name for tool in (await session.list_tools()).tools}
            result = await session.call_tool("activate_project", {"project": "/"})
        return tools, result

    tools, result = anyio.run(scenario)
    assert "activate_project" not in tools
    assert result.is_error


def process_gone(pid: int) -> bool:
    return subprocess.run(["ps", "-p", str(pid)], capture_output=True, check=False).returncode != 0


@pytest.mark.parametrize("victim", ["supervisor_pid", "serena_pid"])
def test_next_client_recovers_from_killed_server(repo, env, victim):
    import signal

    anyio.run(use_serena, repo, env, "codex")
    old = servers(env)[0]
    os.kill(old[victim], signal.SIGKILL)
    time.sleep(1)

    anyio.run(use_serena, repo, env, "codex")
    (new,) = servers(env)
    assert new["serena_pid"] != old["serena_pid"]
    deadline = time.monotonic() + 15
    while not process_gone(old["serena_pid"]) and time.monotonic() < deadline:
        time.sleep(0.5)
    assert process_gone(old["serena_pid"]), "orphaned Serena left running"


@pytest.mark.skipif(shutil.which("lsof") is None, reason="lsof not installed")
def test_server_listens_only_on_private_unix_socket(repo, env):
    async def scenario():
        release = anyio.Event()
        async with anyio.create_task_group() as tg:
            tg.start_soon(use_serena, repo, env, "codex", release)
            with anyio.fail_after(240):
                while not servers(env):
                    await anyio.sleep(0.5)
            (record,) = servers(env)
            tcp = (await anyio.run_process(
                ["lsof", "-a", "-iTCP", "-sTCP:LISTEN", "-p", str(record["serena_pid"])], check=False,
            )).stdout.decode()
            socket_dir = Path(record["socket"]).parent
            release.set()
        return tcp, socket_dir

    tcp, socket_dir = anyio.run(scenario)
    assert tcp == ""
    assert socket_dir.stat().st_mode & 0o777 == 0o700
    assert Path(env["SERENA_SHARED_STATE_DIR"]).stat().st_mode & 0o777 == 0o700


INITIALIZE = json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
               "clientInfo": {"name": "raw", "version": "0"}},
}) + "\n"


def start_raw_client(repo, env) -> subprocess.Popen:
    """A client that, unlike the SDK's, never escalates to SIGKILL on its own."""
    proc = subprocess.Popen(
        [SHIM, "start-mcp-server", "--context=codex", "--project-from-cwd", "--agent-interface", "tools", *NO_DASHBOARD],
        cwd=repo, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    proc.stdin.write(INITIALIZE)
    proc.stdin.flush()
    assert '"result"' in proc.stdout.readline()  # attached and initialized
    return proc


def lease_count(env) -> int:
    return len(list(Path(env["SERENA_SHARED_STATE_DIR"]).glob("*/leases/*")))


@pytest.mark.parametrize("how", ["stdin_eof", "sigterm_with_stdin_open"])
def test_relay_exits_and_releases_its_lease(repo, env, how):
    import signal

    proc = start_raw_client(repo, env)
    assert lease_count(env) == 1
    if how == "stdin_eof":
        proc.stdin.close()
    else:
        proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=15) is not None
    deadline = time.monotonic() + 30
    while servers(env) and time.monotonic() < deadline:  # idle timeout is 3 s
        time.sleep(1)
    assert servers(env) == []


@pytest.mark.parametrize("sig", ["SIGTERM", "SIGKILL"])
def test_supervisor_signalled_during_startup_leaves_no_orphan(repo, env, sig):
    import signal

    state_dir = Path(env["SERENA_SHARED_STATE_DIR"])
    first = subprocess.Popen(
        [SHIM, "start-mcp-server", "--context=codex", "--project-from-cwd", "--agent-interface", "tools", *NO_DASHBOARD],
        cwd=repo, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    deadline = time.monotonic() + 60
    starting = None
    while starting is None and time.monotonic() < deadline:
        for path in state_dir.glob("*/server.json"):
            with contextlib.suppress(ValueError, OSError):
                record = json.loads(path.read_text())
                if record.get("state") == "starting":
                    starting = record
        time.sleep(0.01)
    assert starting, "never saw a starting server"
    os.kill(starting["supervisor_pid"], getattr(signal, sig))

    # The waiting client falls back to a private Serena and still works.
    first.stdin.write(INITIALIZE)
    first.stdin.flush()
    assert '"result"' in first.stdout.readline()
    first.stdin.close()
    first.wait(timeout=30)

    deadline = time.monotonic() + 15
    while not process_gone(starting["serena_pid"]) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert process_gone(starting["serena_pid"]), "Serena orphaned by a supervisor killed during startup"

    # And the project is not wedged: the next client gets a fresh shared server.
    anyio.run(use_serena, repo, env, "codex")
