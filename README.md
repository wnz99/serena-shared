# serena-shared

Run one [Serena](https://github.com/oraios/serena) server per repository and share it between
all your coding-agent sessions (Claude Code, Codex, and other MCP clients), instead of starting
a new Serena and a new set of language servers for every session.

Works on macOS and Linux.

## Why you might want this

Normally each agent session starts its own Serena. Serena in turn starts language servers
(`tsserver`, `pyright`, `gopls`, ...) for the project. If you keep several sessions open in the
same repository, you pay for all of that several times:

- memory: every session holds its own copy of the language servers;
- CPU: every new session indexes the project again, which slows session startup;
- consistency: separate Serena instances can overwrite each other's `.serena/` cache.

Serena can serve several clients from one server, but a server works on one project at a time.
A single global server would let one session switch the project under another session
([Serena docs](https://oraios.github.io/serena/02-usage/020_running.html),
[oraios/serena#1235](https://github.com/oraios/serena/issues/1235)).

serena-shared solves this by starting **one server per repository and per client type**, on
demand, and connecting each session to the right one.

## Before you start

You need:

1. **macOS or Linux.** Windows is not supported.
2. **[uv](https://docs.astral.sh/uv/getting-started/installation/)**, to install the tool.
3. **Serena 2.x, installed as a command.** Check with:

   ```sh
   serena --version
   ```

   If that prints a version, you are ready. If not, install Serena:

   ```sh
   uv tool install serena-agent
   ```

   serena-shared needs the `serena` command to be a regular launcher file, as `uv tool install`
   and `pipx` create. Configurations that run Serena through `uvx --from git+...` each time are
   not shared; they keep working as before. To share a pinned Serena version, point
   `SERENA_SHARED_SERENA` at the `serena` launcher inside that version's environment.

## Install

1. Install serena-shared:

   ```sh
   uv tool install git+https://github.com/wnz99/serena-shared
   ```

2. Check that it is on your `PATH`:

   ```sh
   serena-shared --version
   ```

   This prints Serena's version, because serena-shared passes any command it does not handle
   straight to Serena. If the command is not found, run `uv tool update-shell` and open a new
   terminal.

## Set up your agents

serena-shared is a drop-in replacement for the `serena` command. In each agent's MCP settings,
change the command from `serena` to `serena-shared` and keep the arguments as they are. The
arguments must include `--project-from-cwd`; that is what tells serena-shared which repository
a session belongs to.

### Claude Code

If you already have a Serena server configured, remove it first:

```sh
claude mcp remove serena -s user
```

Then add it back through serena-shared:

```sh
claude mcp add serena -s user -- serena-shared start-mcp-server --context=claude-code --project-from-cwd
```

### Codex

In `~/.codex/config.toml`, set the Serena entry to:

```toml
[mcp_servers.serena]
command = "serena-shared"
args = ["start-mcp-server", "--context=codex", "--project-from-cwd"]
```

If you already have extra Serena arguments there (for example `--agent-interface tools`), keep
them. Codex gives an MCP server 10 seconds to start by default. On a slow or busy machine you
can allow more with `startup_timeout_sec = 30` in the same section.

### Other MCP clients

Use the same pattern: command `serena-shared`, arguments as you would pass to `serena`,
including `start-mcp-server`, the right `--context` for your client, and `--project-from-cwd`.

### Apply the change

Restart your agent sessions. Sessions that are already running keep their old Serena until
they restart.

## Check that it works

1. Open two sessions in the same repository.
2. Look for the shared server:

   ```sh
   ls ~/.local/state/serena-shared/
   cat ~/.local/state/serena-shared/*/server.json
   ```

   You should see one `server.json` per repository and client type, with `"state": "running"`.
   Two Claude Code sessions in the same repository share one entry. A Claude Code session and
   a Codex session in the same repository get one entry each, because each client type uses a
   different Serena context and tool set.

3. For Claude Code, `claude mcp get serena` should show `Status: ✔ Connected`.

If a session falls back to a private Serena, serena-shared prints the reason to stderr, which
your agent shows in its MCP logs.

## Settings

All settings are optional environment variables. Set them in the agent's MCP configuration
(the `env` section) or in your shell.

| Variable | Default | What it does |
|---|---|---|
| `SERENA_SHARED_SERENA` | `serena` on your `PATH` | Path to the Serena command to run |
| `SERENA_SHARED_IDLE_SECONDS` | `1800` (30 minutes) | How long a server keeps running after its last session closes |
| `SERENA_SHARED_STATE_DIR` | `~/.local/state/serena-shared` (or `$XDG_STATE_HOME/serena-shared`) | Where sockets, locks and logs are kept. It must be a folder that belongs to you. Keep the path short: Unix socket paths are limited to about 100 characters |

Each server writes its log to `<state dir>/<server id>/server.log`. When a server starts and
the log is larger than 5 MB, the old log is moved to `server.log.1`.

Options that control how the shared server is reached (`--transport`, `--host`, `--port`,
`--project`), Serena's web dashboard options and `--enable-gui-log-window` are set by
serena-shared; any values you pass for them are ignored.

## Upgrade

Reinstall from the repository to get the latest version:

```sh
uv tool install --reinstall git+https://github.com/wnz99/serena-shared
```

Then restart your agent sessions. Servers started by the previous version keep running for
their existing sessions and stop on their own after the idle timeout.

## Uninstall

1. Change your agents' MCP settings back from `serena-shared` to `serena`, and restart them.
2. Remove the tool:

   ```sh
   uv tool uninstall serena-shared
   ```

3. Optionally delete its state directory: `rm -rf ~/.local/state/serena-shared`. Running servers
   stop on their own after the idle timeout.

## How it works

When an agent starts `serena-shared start-mcp-server --project-from-cwd ...`, serena-shared:

1. Finds the repository root the same way Serena does: the nearest folder, going up from the
   session's folder, that contains `.serena/project.yml` or `.git`. Git worktrees count as
   their own repositories.
2. Prepares a copy of the requested Serena context with `single_project: true`, so the shared
   server does not offer `activate_project` and no session can switch it to another project.
3. Connects to the running server for this repository, Serena installation and set of
   arguments, or starts one. Different contexts, such as `claude-code` and `codex`, get
   separate servers so each client gets its own tool set.
4. Relays messages between the agent (over stdin/stdout, as before) and the server.

Each session holds a lock while it is connected. When no session has been connected for the
idle timeout, the server stops together with its language servers. If a server or its
supervisor crashes, the next session start cleans it up and starts a fresh one.

If a shared server cannot be started or reached within a few seconds, the session runs its own
private Serena, as it would without serena-shared, and the reason is printed to stderr. A
server that is merely slow to start keeps starting in the background, and later sessions use it.

Any other use of the command (for example `serena-shared --version`, or a configuration without
`--project-from-cwd`) runs Serena directly, unchanged.

## Security

Shared servers listen on a Unix socket inside the state directory, which only your user can
access (permissions `0700`). They do not open a network port: serena-shared turns off Serena's
web dashboard for shared servers, because a background server has no use for it and the
dashboard would listen on a local TCP port. Serena's MCP server also keeps its protection
against requests from web pages (DNS-rebinding protection).

serena-shared refuses to use a state directory that is a symlink or belongs to another user,
and runs a private Serena instead.

## Limitations

- Serena handles one tool call at a time per server. Sessions sharing a server wait for each
  other's long-running calls.
- A server keeps the environment (for example `PATH`) of the session that started it.
- If a server stops while sessions are connected, those sessions lose Serena until they restart.
- State folders for repositories or worktrees you have deleted are not removed automatically.
  Delete them from the state directory when no server is running.
- serena-shared starts Serena through its normal command-line entry point but asks the web server
  (uvicorn) to listen on a Unix socket. A future Serena release that stops using uvicorn would
  require an update to serena-shared.

## Development

```sh
uv sync
uv run pytest -q
```

The integration tests start real Serena servers and need `serena` on your `PATH`. They take
about a minute.

## License

MIT. See [LICENSE](LICENSE).
