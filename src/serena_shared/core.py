"""Project-root detection, argument rewriting, and per-server state on disk."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import secrets
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TypedDict

# Serena still gets `--host 127.0.0.1` so the MCP SDK enables DNS-rebinding protection; the
# client sends a matching Host header over the Unix socket.
HOST = "127.0.0.1"
HTTP_BASE = f"http://{HOST}:1"
SERVE_UNIX = Path(__file__).with_name("serve_unix.py")
MAX_SOCKET_PATH = 100  # bytes; AF_UNIX allows 104 on macOS, 108 on Linux
DEFAULT_CONTEXT = "desktop-app"  # Serena's own default

# Serena options the shim owns; client-supplied values are dropped.
_OWNED_FLAGS = {
    "--transport", "--host", "--port", "--project", "--project-file", "--context",
    # A background shared server must not open a dashboard port or a browser tab.
    "--enable-web-dashboard", "--open-web-dashboard",
}
_OWNED_SWITCHES = {"--project-from-cwd"}


class NotShareable(Exception):
    """Sharing is unavailable; the reason is the message. The caller runs plain Serena."""


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


def serena_python(serena: str) -> str:
    """Serena's interpreter, from its launcher's shebang."""
    try:
        with open(os.path.realpath(serena), "rb") as f:
            shebang = f.readline().decode().strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise NotShareable(f"cannot read {serena}: {exc}") from exc
    interpreter = shebang[2:]
    if (
        not shebang.startswith("#!/")
        or " " in interpreter
        or not os.path.basename(interpreter).startswith("python")
    ):
        raise NotShareable(f"{serena} is not a Python launcher with a direct interpreter path")
    return interpreter


def pinned_context(python: str, context: str) -> Path:
    """Write a copy of `context` with `single_project: true`, which makes Serena drop
    `activate_project` from the exposed tools. A shared server must never switch projects:
    the switch would apply to every connected client.

    Runs Serena's own interpreter so contexts resolve exactly as Serena resolves them."""
    if context.endswith((".yml", ".yaml")):
        context = os.path.abspath(context)
    try:
        result = subprocess.run(
            [python, "-I", "-c", _PIN_CONTEXT, context],
            capture_output=True, text=True, timeout=60, check=True, cwd=state_root(),
        )
    except subprocess.CalledProcessError as exc:
        raise NotShareable(f"cannot load Serena context {context!r}: {exc.stderr.strip()[-300:]}") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise NotShareable(f"cannot run Serena's interpreter: {exc}") from exc
    contexts = private_dir(state_root() / "contexts")
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
        "--enable-web-dashboard", "false",
        "--open-web-dashboard", "false",
    ]
    return out


def server_key(serena: str, args: Sequence[str]) -> str:
    """Servers are shared only between clients that would have started an identical server."""
    material = json.dumps(
        [os.path.realpath(serena), list(args), os.environ.get("SERENA_HOME", "")],
    )
    return hashlib.sha256(material.encode()).hexdigest()[:20]


def private_dir(path: Path) -> Path:
    """Create `path` if needed and insist it is a real directory owned by us with mode 0700.
    A directory someone else created (e.g. under /tmp) or a symlink is refused."""
    with contextlib.suppress(FileExistsError):
        path.mkdir(mode=0o700, parents=True)
    info = os.lstat(path)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise NotShareable(f"{path} is not a directory owned by the current user")
    if stat.S_IMODE(info.st_mode) != 0o700:
        os.chmod(path, 0o700)
    return path


def state_root() -> Path:
    base = os.environ.get("SERENA_SHARED_STATE_DIR") or os.path.join(
        os.environ.get("XDG_STATE_HOME") or os.path.expanduser("~/.local/state"), "serena-shared"
    )
    return private_dir(Path(base))


def kill_group(pid: int, sig: int) -> None:
    if pid > 0:  # 0 and negatives address other process groups
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(pid, sig)


def socket_ready(path: str, timeout: float = 0.5) -> bool:
    with contextlib.suppress(OSError), socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(timeout)
        sock.connect(path)
        return True
    return False


def is_locked(path: Path) -> bool:
    """True when another open file description holds an flock on `path`."""
    try:
        # Writable (flock over NFS needs it) but never created: a probe must not resurrect
        # a file someone just removed.
        fd = os.open(path, os.O_WRONLY | os.O_APPEND)
    except FileNotFoundError:
        return False
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    except OSError:
        return True  # cannot tell; assume held rather than reap a live holder
    finally:
        os.close(fd)
    return False


class ServerRecord(TypedDict, total=False):
    state: str  # "starting" | "running"
    supervisor_pid: int
    serena_pid: int
    socket: str
    root: str


@dataclass(frozen=True)
class ServerState:
    """Files for one shared server: lock, supervisor.lock, server.lock, spec, server.json,
    leases/, log.

    Protocol:
    * Clients read and modify `server.json` only while holding `lock`. The supervisor writes
      it while the client that started it holds `lock` and waits.
    * The supervisor holds an flock on `supervisor.lock` for its whole life: a server is
      healthy only while its supervisor (and so its idle reaper) is alive.
    * The supervisor also locks `server.lock` and shares that open file with Serena, so it
      stays held while any process of this server is alive. PIDs in `server.json` are trusted
      for signalling only while it is held, which rules out signalling a reused PID."""

    dir: Path

    @classmethod
    def for_key(cls, key: str) -> ServerState:
        state = cls(private_dir(state_root() / key))
        private_dir(state.leases)
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
    def supervisor_lock(self) -> Path:
        return self.dir / "supervisor.lock"

    @property
    def server_lock(self) -> Path:
        return self.dir / "server.lock"

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

    def supervisor_alive(self) -> bool:
        return is_locked(self.supervisor_lock)

    def server_alive(self) -> bool:
        """Any process of this server (supervisor, Serena) still running."""
        return is_locked(self.server_lock)

    def healthy(self, record: ServerRecord | None) -> bool:
        return bool(
            record
            and record.get("state") == "running"
            and self.supervisor_alive()
            and socket_ready(record.get("socket", ""))
        )

    def discard(self, record: ServerRecord) -> None:
        """Stop whatever an unhealthy record still points at and remove it. Caller holds `lock`."""
        if self.server_alive():  # the PIDs still belong to this server
            supervisor_pid = record.get("supervisor_pid", 0)
            if supervisor_pid > 0:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.kill(supervisor_pid, signal.SIGTERM)
            kill_group(record.get("serena_pid", 0), signal.SIGKILL)
            deadline = time.monotonic() + 10
            while self.server_alive() and time.monotonic() < deadline:
                time.sleep(0.1)
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
            if is_locked(lease):
                count += 1
            else:
                lease.unlink(missing_ok=True)
        return count
