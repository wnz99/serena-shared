import os

from serena_shared.core import (
    ServerState,
    context_arg,
    find_project_root,
    is_shareable,
    server_args,
    server_key,
)


def test_root_is_nearest_git_or_serena_project(tmp_path):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    worktree = repo / ".worktrees" / "feature"
    worktree.mkdir(parents=True)
    (worktree / ".git").write_text("gitdir: ../../.git/worktrees/feature\n")
    assert find_project_root(repo / ".worktrees") == repo
    assert find_project_root(worktree) == worktree
    assert find_project_root(tmp_path) is None


def test_only_stdio_project_from_cwd_is_shared():
    assert is_shareable(["start-mcp-server", "--context=codex", "--project-from-cwd"])
    assert is_shareable(["start-mcp-server", "--project-from-cwd", "--transport", "stdio"])
    assert not is_shareable(["start-mcp-server", "--project", "/x"])
    assert not is_shareable(["start-mcp-server", "--project-from-cwd", "--transport=sse"])
    assert not is_shareable(["--version"])


def test_server_args_pin_project_context_and_own_transport(tmp_path):
    ctx = tmp_path / "ctx.yml"
    args = server_args(
        ["start-mcp-server", "--context=codex", "--project-from-cwd", "--port", "9", "--host=0.0.0.0",
         "--log-level", "WARNING"],
        tmp_path,
        ctx,
    )
    assert args == [
        "start-mcp-server", "--log-level", "WARNING",
        "--context", str(ctx), "--project", str(tmp_path), "--transport", "streamable-http",
        "--host", "127.0.0.1",
    ]


def test_context_arg_forms():
    assert context_arg(["--context", "codex"]) == "codex"
    assert context_arg(["--context=claude-code"]) == "claude-code"
    assert context_arg([]) == "desktop-app"


def test_key_separates_contexts_and_serena_home(monkeypatch):
    codex = server_key("/bin/sh", ["start-mcp-server", "--context=codex"])
    claude = server_key("/bin/sh", ["start-mcp-server", "--context=claude-code"])
    assert codex != claude
    monkeypatch.setenv("SERENA_HOME", "/elsewhere")
    assert server_key("/bin/sh", ["start-mcp-server", "--context=codex"]) != codex


def test_only_locked_leases_count(tmp_path, monkeypatch):
    monkeypatch.setenv("SERENA_SHARED_STATE_DIR", str(tmp_path))
    state = ServerState.for_key("k")
    lease = state.take_lease()
    (state.leases / "stale").touch()  # left behind by a client that died
    assert state.live_leases() == 1
    assert not (state.leases / "stale").exists()
    lease.close()  # what the kernel does when the client dies
    assert state.live_leases() == 0
    assert oct(tmp_path.stat().st_mode & 0o777) == "0o700"


def test_concurrent_context_pins_do_not_collide(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from serena_shared.core import atomic_write

    target = tmp_path / "ctx.yml"
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda _: atomic_write(target, "same"), range(64)))
    assert target.read_text() == "same"
    assert [p.name for p in tmp_path.iterdir()] == ["ctx.yml"]
