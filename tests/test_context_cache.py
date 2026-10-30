"""Cache correctness matters more than cache hit counts: stale contexts change tools."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from serena_shared import core


@pytest.fixture
def context_source(tmp_path, monkeypatch):
    monkeypatch.setenv("SERENA_SHARED_STATE_DIR", str(tmp_path / "state"))
    source = tmp_path / "codex.yml"
    source.write_text("prompt: original\n")
    override = tmp_path / "override.yml"
    calls = []

    def load(*args, **kwargs):
        calls.append(args)
        selected = override if override.exists() else source
        # A pinned context has the same wire tool policy as Serena's own loader.
        text = selected.read_text() + "single_project: true\n"
        return SimpleNamespace(stdout=json.dumps({
            "text": text,
            "dependencies": {str(p): core._fingerprint(str(p)) for p in (source, override)},
        }))

    monkeypatch.setattr(core.subprocess, "run", load)
    return source, override, calls


def test_unchanged_context_does_not_launch_serena_again(context_source):
    _, _, calls = context_source
    first = core.pinned_context("/bin/sh", "codex")
    assert core.pinned_context("/bin/sh", "codex") == first
    assert len(calls) == 1  # the performance contract: no interpreter on a warm client
    assert "single_project: true" in first.read_text()


def test_source_edit_or_new_override_invalidates_pinned_context(context_source):
    source, override, _ = context_source
    original = core.pinned_context("/bin/sh", "codex")
    source.write_text("prompt: changed\n")
    changed = core.pinned_context("/bin/sh", "codex")
    assert changed != original
    assert "changed" in changed.read_text()
    override.write_text("prompt: custom\n")
    custom = core.pinned_context("/bin/sh", "codex")
    assert "custom" in custom.read_text()
    override.unlink()
    assert core.pinned_context("/bin/sh", "codex") == changed


@pytest.mark.parametrize("corrupt", [b"single_project: false\n", b"\xff"])
def test_corrupt_target_or_manifest_is_regenerated(context_source, corrupt):
    _, _, calls = context_source
    target = core.pinned_context("/bin/sh", "codex")
    target.write_bytes(corrupt)
    assert "single_project: true" in core.pinned_context("/bin/sh", "codex").read_text()
    manifests = list(target.parent.glob("request-*.json"))
    assert len(manifests) == 1
    manifests[0].write_text('{"target":"../../untrusted.yml","dependencies":{}}')
    assert core.pinned_context("/bin/sh", "codex") == target
    assert len(calls) == 3


def test_home_and_context_identity_do_not_share_cached_results(context_source, monkeypatch):
    _, _, calls = context_source
    core.pinned_context("/bin/sh", "codex")
    monkeypatch.setenv("SERENA_HOME", "/different/home")
    core.pinned_context("/bin/sh", "codex")
    core.pinned_context("/bin/sh", "claude-code")
    assert len(calls) == 3
