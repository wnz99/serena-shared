"""Project-root detection, argument rewriting, and per-server state on disk."""

from __future__ import annotations

import contextlib
import errno
import fcntl
import hashlib
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TypedDict

# Serena still gets `--host 127.0.0.1` so the MCP SDK enables DNS-rebinding protection; the
# client sends a matching Host header over the Unix socket.
HOST = "127.0.0.1"
HTTP_BASE = f"http://{HOST}:1"
SERVE_UNIX = Path(__file__).with_name("serve_unix.py")
MAX_SOCKET_PATH = 100  # AF_UNIX path limit: 104 bytes on macOS, 108 on Linux
DEFAULT_CONTEXT = "desktop-app"  # Serena's own default

# Serena options the shim owns; client-supplied values are dropped.
_OWNED_FLAGS = {"--transport", "--host", "--port", "--project", "--project-file", "--context"}
_OWNED_SWITCHES = {"--project-from-cwd"}


def warn(message: str) -> None:
    print(f"serena-shared: {message}", file=sys.stderr)


def find_project_root(start: Path) -> Path | None:
    """Mirror `serena start-mcp-server --project-from-cwd`: nearest ancestor holding
    `.serena/project.yml` or `.git` (directory or worktree pointer file)."""
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".serena" / "project.yml").is_file() or (candidate / ".git").exists():
            return candidate
    return None


def is_shareable(args: Sequence[str]) -> bool:
    """Only `start-mcp-server --project-from-cwd` over stdio is rewritten; anything else passes through."""
    if not args or args[0] != "start-mcp-server" or "--project-from-cwd" not in args:
        return False
    for i, arg in enumerate(args):
        if arg == "--transport" and i + 1 < len(args) and args[i + 1] != "stdio":
            return False
        if arg.startswith("--transport=") and arg.split("=", 1)[1] != "stdio":
            return False
    return True


def context_arg(args: Sequence[str]) -> str:
    for i, arg in enumerate(args):
        if arg == "--context" and i + 1 < len(args):
            return args[i + 1]
        if arg.startswith("--context="):
            return arg.split("=", 1)[1]
    return DEFAULT_CONTEXT


_PIN_CONTEXT = """
import sys, yaml
from serena.config.context_mode import SerenaAgentContext
context = SerenaAgentContext.load(sys.argv[1])
path = context._yaml_path or SerenaAgentContext.get_path(context.name)
with open(path, encoding="utf-8") as f:
    data = yaml.safe_load(f)
data["name"] = context.name
data["single_project"] = True
sys.stdout.write(yaml.safe_dump(data, sort_keys=False, allow_unicode=True))
"""


def atomic_write(path: Path, text: str) -> None:
    """Write via a unique temporary file so concurrent writers never share one."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def serena_python(serena: str) -> str | None:
    """Serena's interpreter, from its launcher's shebang. None, with a reason on stderr,
    when the launcher is not a Python script with a direct interpreter path."""
    try:
        with open(os.path.realpath(serena), "rb") as f:
            shebang = f.readline().decode().strip()
    except (OSError, UnicodeDecodeError) as exc:
        warn(f"not sharing: cannot read {serena}: {exc}")
        return None
    if not shebang.startswith("#!/") or " " in shebang:
        warn(f"not sharing: {serena} has no direct interpreter shebang")
        return None
    return shebang[2:]


def pinned_context(python: str, context: str) -> Path | None:
    """Write a copy of `context` with `single_project: true`, which makes Serena drop
    `activate_project` from the exposed tools. A shared server must never switch projects:
    the switch would apply to every connected client.

    Runs Serena's own interpreter so contexts resolve exactly as Serena resolves them.
    Returns None, with a reason on stderr, when that is not possible; the caller then falls
    back to a private stdio server."""
    if context.endswith((".yml", ".yaml")):
        context = os.path.abspath(context)
    try:
        result = subprocess.run(
            [python, "-I", "-c", _PIN_CONTEXT, context],
            capture_output=True, text=True, timeout=60, check=True, cwd=state_root(),
        )
    except subprocess.CalledProcessError as exc:
        warn(f"not sharing: cannot load Serena context {context!r}: {exc.stderr.strip()[-300:]}")
        return None
    except (OSError, subprocess.SubprocessError) as exc:
        warn(f"not sharing: cannot run Serena's interpreter: {exc}")
        return None
    contexts = state_root() / "contexts"
    contexts.mkdir(mode=0o700, exist_ok=True)
    target = contexts / f"{hashlib.sha256(result.stdout.encode()).hexdigest()[:20]}.yml"
    if not target.exists():  # content-addressed: concurrent writers write identical bytes
        atomic_write(target, result.stdout)
    return target


def server_args(args: Sequence[str], root: Path, context_file: Path) -> list[str]:
    """Rewrite client args into args for a shared streamable-http server pinned to `root`."""
    out: list[str] = []
    skip = False
    for arg in args:
        if skip:
            skip = False
            continue
        name = arg.split("=", 1)[0]
        if arg in _OWNED_SWITCHES:
            continue
        if name in _OWNED_FLAGS:
            skip = "=" not in arg
            continue
        out.append(arg)
    out += [
        "--context", str(context_file),
        "--project", str(root),
        "--transport", "streamable-http",
        "--host", HOST,
    ]
    return out


def server_key(serena: str, args: Sequence[str]) -> str:
    """Servers are shared only between clients that would have started an identical server."""
    material = json.dumps(
        [os.path.realpath(serena), list(args), os.environ.get("SERENA_HOME", "")],
    )
    return hashlib.sha256(material.encode()).hexdigest()[:20]


def state_root() -> Path:
    base = os.environ.get("SERENA_SHARED_STATE_DIR") or os.path.join(
        os.environ.get("XDG_STATE_HOME") or os.path.expanduser("~/.local/state"), "serena-shared"
    )
    path = Path(base)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def pid_alive(pid: int) -> bool:
    if pid <= 0:  # 0 and negatives address process groups, not a process
        return False
    try:
        os.kill(pid, 0)
    except OSError as exc:
        return exc.errno == errno.EPERM
    return True


def kill_group(pid: int, sig: int) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, sig)


def socket_ready(path: str, timeout: float = 0.5) -> bool:
    with contextlib.suppress(OSError), socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(timeout)
        sock.connect(path)
        return True
    return False


class ServerRecord(TypedDict, total=False):
    state: str  # "starting" | "running"
    supervisor_pid: int
    serena_pid: int
    socket: str
    root: str


@dataclass(frozen=True)
class ServerState:
    """Files for one shared server: lock, spec, server.json, leases/, log.

    Protocol: clients read and modify `server.json` only while holding the lock. The
    supervisor writes it while the client that started it holds the lock and waits, and
    deletes it only under the lock and only when the record names that supervisor."""

    dir: Path

    @classmethod
    def for_key(cls, key: str) -> ServerState:
        state = cls(state_root() / key)
        state.dir.mkdir(mode=0o700, exist_ok=True)
        state.leases.mkdir(mode=0o700, exist_ok=True)
        return state

    @property
    def socket(self) -> Path:
        return self.dir / "serena.sock"

    @property
    def leases(self) -> Path:
        return self.dir / "leases"

    @property
    def spec_file(self) -> Path:
        return self.dir / "spec.json"

    @property
    def server_file(self) -> Path:
        return self.dir / "server.json"

    @property
    def log_file(self) -> Path:
        return self.dir / "server.log"

    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        with open(self.dir / "lock", "a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def write_json(self, path: Path, data: dict) -> None:
        atomic_write(path, json.dumps(data))

    def read_server(self) -> ServerRecord | None:
        try:
            return json.loads(self.server_file.read_text())
        except (OSError, ValueError):
            return None

    def healthy(self, record: ServerRecord | None) -> bool:
        return bool(
            record
            and record.get("state") == "running"
            and pid_alive(record.get("supervisor_pid", 0))
            and pid_alive(record.get("serena_pid", 0))
            and socket_ready(record.get("socket", ""))
        )

    def discard(self, record: ServerRecord) -> None:
        """Stop whatever an unhealthy record still points at and remove it. Caller holds the lock."""
        if pid_alive(record.get("supervisor_pid", 0) or 0):
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(record["supervisor_pid"], signal.SIGTERM)
        if record.get("serena_pid"):
            kill_group(record["serena_pid"], signal.SIGKILL)
        self.server_file.unlink(missing_ok=True)

    def remove_record_if_owned(self, supervisor_pid: int) -> None:
        record = self.read_server()
        if record and record.get("supervisor_pid") == supervisor_pid:
            self.server_file.unlink(missing_ok=True)

    def take_lease(self) -> IO[str]:
        """Hold an exclusive lock on a fresh lease file for this client's lifetime. The kernel
        drops the lock when the process dies, however it dies, so liveness never depends on
        PIDs that may be reused."""
        name = f"{os.getpid()}-{secrets.token_hex(4)}"
        handle = open(self.dir / f".{name}", "w")
        fcntl.flock(handle, fcntl.LOCK_EX)
        # Publish only once locked, so the reaper never sees an unlocked live lease.
        os.rename(self.dir / f".{name}", self.leases / name)
        return handle

    def live_leases(self) -> int:
        """Count leases still locked by a live client; remove the rest."""
        count = 0
        for lease in list(self.leases.iterdir()):
            try:
                with open(lease) as handle:
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        count += 1
                        continue
                    lease.unlink(missing_ok=True)
            except FileNotFoundError:
                continue
        return count
