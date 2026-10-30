"""Project-root detection, argument rewriting, and per-server state on disk."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
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
STOP_GRACE_SECONDS = 10
DISCARD_STEP_SECONDS = 1.5  # discard runs while clients wait; keep it well under their timeouts
MAX_LOG_BYTES = 5 * 1024 * 1024
STRAY_LEASE_SECONDS = 60
# Part of every server key: bump on any change to the on-disk protocol or supervisor
# behaviour, so servers started by an older serena-shared keep their own state directory,
# serve only their existing sessions and idle out undisturbed.
STATE_VERSION = 3

# Serena options the shim owns; client-supplied values are dropped.
_OWNED_FLAGS = {
    "--transport", "--host", "--port", "--project", "--project-file", "--context",
    # A background shared server must not open a dashboard port, a browser tab or a window.
    "--enable-web-dashboard", "--open-web-dashboard", "--enable-gui-log-window",
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
import json, os, site, sys, yaml
from pathlib import Path
from serena.config import context_mode, serena_config
from serena import constants
from serena.config.context_mode import SerenaAgentContext
context = SerenaAgentContext.load(sys.argv[1])
path = context._yaml_path or SerenaAgentContext.get_path(context.name)
dependencies = [str(Path(path).absolute()), context_mode.__file__, serena_config.__file__, constants.__file__,
                str(Path(serena_config.SerenaPaths().user_contexts_dir).absolute() / (context.name + ".yml"))]
for directory in site.getsitepackages():
    dependencies.append(directory)
    dependencies.extend(str(p) for p in Path(directory).glob("*.pth"))
dependencies.append(str(Path(sys.prefix) / "pyvenv.cfg"))
def fingerprint(path):
    try:
        s = os.stat(path)
        return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns]
    except FileNotFoundError:
        return None
before = {p: fingerprint(p) for p in dependencies}
with open(path, encoding="utf-8") as f:
    data = yaml.safe_load(f)
data["name"] = context.name
data["single_project"] = True
if before != {p: fingerprint(p) for p in dependencies}:
    raise RuntimeError("Serena context changed while it was being pinned")
sys.stdout.write(json.dumps({"text": yaml.safe_dump(data, sort_keys=False, allow_unicode=True), "dependencies": before}))
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


# Second line of the `#!/bin/sh` launchers pip and uv write when the interpreter path is too
# long for a shebang: the interpreter is either next to the launcher or an absolute path.
_TRAMPOLINE_RELATIVE = re.compile(
    r"""^'''exec' "\$\(dirname -- "\$\(realpath -- "\$0"\)"\)"/'([^'/]+)' "\$0" "\$@"$"""
)
_TRAMPOLINE_ABSOLUTE = re.compile(r"""^'''exec' '(/[^']+)' "\$0" "\$@"$""")


def serena_python(serena: str) -> str:
    """Serena's interpreter, from its launcher's shebang or sh trampoline."""
    launcher = os.path.realpath(serena)
    try:
        with open(launcher, "rb") as f:
            first, second = f.readline().decode().strip(), f.readline().decode().strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise NotShareable(f"cannot read {serena}: {exc}") from exc
    interpreter = first[2:]
    if first == "#!/bin/sh":
        if match := _TRAMPOLINE_RELATIVE.match(second):
            interpreter = os.path.join(os.path.dirname(launcher), match.group(1))
        elif match := _TRAMPOLINE_ABSOLUTE.match(second):
            interpreter = match.group(1)
    if (
        not first.startswith("#!/")
        or " " in interpreter
        or not os.path.basename(interpreter).startswith("python")
    ):
        raise NotShareable(f"{serena} is not a Python launcher with a recognisable interpreter")
    return interpreter


def _fingerprint(path: str) -> list[int] | None:
    try:
        info = os.stat(path)
        return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]
    except FileNotFoundError:
        return None


def _cached_context(cache: Path, contexts: Path) -> Path | None:
    try:
        entry = json.loads(cache.read_text())
        name = entry["target"]
        if not isinstance(name, str) or not re.fullmatch(r"[0-9a-f]{20}\.yml", name):
            return None
        dependencies = entry["dependencies"]
        if not isinstance(dependencies, dict) or not dependencies:
            return None
        if any(_fingerprint(path) != stamp for path, stamp in dependencies.items()):
            return None
        target = contexts / name
        if hashlib.sha256(target.read_bytes()).hexdigest()[:20] != name[:-4]:
            return None
        return target
    except (OSError, ValueError, KeyError, TypeError):
        return None


def pinned_context(python: str, context: str) -> Path:
    """Write a copy of `context` with `single_project: true`, which makes Serena drop
    `activate_project` from the exposed tools. A shared server must never switch projects:
    the switch would apply to every connected client.

    Runs Serena's own interpreter so contexts resolve exactly as Serena resolves them.
    Reuse its result while the source, resolver, interpreter and override remain unchanged."""
    if os.sep in context or (os.altsep and os.altsep in context) or context.lower().endswith((".yml", ".yaml")):
        context = os.path.abspath(context)
    contexts = private_dir(state_root() / "contexts")
    identity = json.dumps([python, os.path.realpath(python), _fingerprint(python), context,
                           os.environ.get("SERENA_HOME", ""), str(Path.home()), _PIN_CONTEXT])
    cache = contexts / f"request-{hashlib.sha256(identity.encode()).hexdigest()[:20]}.json"
    if cached := _cached_context(cache, contexts):
        return cached
    try:
        result = subprocess.run(
            [python, "-I", "-c", _PIN_CONTEXT, context],
            capture_output=True, text=True, timeout=60, check=True, cwd=state_root(),
        )
    except subprocess.CalledProcessError as exc:
        raise NotShareable(f"cannot load Serena context {context!r}: {exc.stderr.strip()[-300:]}") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise NotShareable(f"cannot run Serena's interpreter: {exc}") from exc
    try:
        payload = json.loads(result.stdout)
        text = payload["text"]
        dependencies = payload["dependencies"]
        if not isinstance(text, str) or not isinstance(dependencies, dict) or not dependencies:
            raise ValueError("invalid context metadata")
    except (ValueError, KeyError, TypeError) as exc:
        raise NotShareable("Serena returned invalid pinned context metadata") from exc
    target = contexts / f"{hashlib.sha256(text.encode()).hexdigest()[:20]}.yml"
    if not target.exists() or target.read_bytes() != text.encode():
        atomic_write(target, text)
    # A concurrent edit after the subprocess read must invalidate the next request too.
    if all(_fingerprint(path) == stamp for path, stamp in dependencies.items()):
        atomic_write(cache, json.dumps({"target": target.name, "dependencies": dependencies}))
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
        "--enable-gui-log-window", "false",
    ]
    return out


def server_key(serena: str, args: Sequence[str]) -> str:
    """Servers are shared only between clients that would have started an identical server."""
    material = json.dumps(
        [STATE_VERSION, os.path.realpath(serena), list(args), os.environ.get("SERENA_HOME", "")],
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


def _signal(pid: int, sig: int) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.kill(pid, sig)


def _wait_for(condition, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)
    return True


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
    def locked(self, deadline: float | None = None) -> Iterator[None]:
        """Hold `lock`. With a `time.monotonic()` deadline, raise NotShareable once it passes
        instead of queueing behind other clients indefinitely."""
        with open(self.dir / "lock", "a") as handle:
            if deadline is None:
                fcntl.flock(handle, fcntl.LOCK_EX)
            else:
                while True:
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise NotShareable("another session is still setting up the shared server") from None
                        time.sleep(0.05)
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
        """Stop whatever an unhealthy record still points at and remove it. Caller holds `lock`.

        Each PID is signalled only while a lock proves it still belongs to this server. The
        supervisor goes first; once supervisor.lock is free, a held server.lock means Serena
        (which inherited it) is alive, so its process group still exists and cannot have been
        reused. Language servers do not inherit server.lock: if Serena has already exited they
        are not detected here; Serena and the supervisor normally stop them."""
        supervisor_pid = record.get("supervisor_pid", 0)
        if supervisor_pid > 0 and self.supervisor_alive():
            _signal(supervisor_pid, signal.SIGTERM)
            if not _wait_for(lambda: not self.supervisor_alive(), DISCARD_STEP_SECONDS):
                if self.supervisor_alive():
                    _signal(supervisor_pid, signal.SIGKILL)
                _wait_for(lambda: not self.supervisor_alive(), DISCARD_STEP_SECONDS)
        if self.server_alive() and not self.supervisor_alive():
            kill_group(record.get("serena_pid", 0), signal.SIGKILL)
            _wait_for(lambda: not self.server_alive(), DISCARD_STEP_SECONDS)
        if self.server_alive():
            # Keep the record: its PIDs are the only way a later attempt can stop these processes.
            raise NotShareable(f"processes of a previous server are still running; see {self.log_file}")
        self.server_file.unlink(missing_ok=True)

    def rotate_log(self) -> None:
        """Called before a supervisor starts, while nothing has the log open for writing."""
        with contextlib.suppress(FileNotFoundError):
            if self.log_file.stat().st_size > MAX_LOG_BYTES:
                os.replace(self.log_file, self.log_file.with_suffix(".log.1"))

    def remove_record_if_owned(self, supervisor_pid: int) -> None:
        record = self.read_server()
        if record and record.get("supervisor_pid") == supervisor_pid:
            self.server_file.unlink(missing_ok=True)

    def take_lease(self) -> IO[str]:
        """Hold an exclusive lock on a fresh lease file for this client's lifetime. The kernel
        drops the lock when the process dies, however it dies, so liveness never depends on
        PIDs that may be reused."""
        name = f"{os.getpid()}-{secrets.token_hex(4)}"
        handle = open(self.leases / f".{name}", "w")  # noqa: SIM115 - held for the client lifetime
        fcntl.flock(handle, fcntl.LOCK_EX)
        # Publish only once locked, so the reaper never sees an unlocked live lease.
        os.rename(self.leases / f".{name}", self.leases / name)
        return handle

    def live_leases(self) -> int:
        """Count leases still locked by a live client; remove the rest."""
        count = 0
        for lease in list(self.leases.iterdir()):
            if lease.name.startswith("."):  # not yet published, or left by a client that died then
                with contextlib.suppress(FileNotFoundError):
                    stale = time.time() - lease.stat().st_mtime > STRAY_LEASE_SECONDS
                    if stale and not is_locked(lease):
                        lease.unlink(missing_ok=True)
                continue
            if is_locked(lease):
                count += 1
            else:
                lease.unlink(missing_ok=True)
        return count
