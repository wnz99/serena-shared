"""Run Serena's own CLI, serving streamable HTTP on a Unix socket instead of a TCP port.

Executed by Serena's interpreter (not serena-shared's), as
`python -I serve_unix.py <socket> start-mcp-server ... --transport streamable-http --host 127.0.0.1`.
Serena and the MCP SDK build the server exactly as usual, including DNS-rebinding
protection for 127.0.0.1; only uvicorn's bind target changes. Access is then limited by the
permissions of the socket's directory.
"""

import sys

import uvicorn

_socket_path = sys.argv[1]
_original_init = uvicorn.Config.__init__


def _init_with_unix_socket(self, *args, **kwargs):
    kwargs["uds"] = _socket_path  # uvicorn binds the socket instead of host:port
    _original_init(self, *args, **kwargs)


uvicorn.Config.__init__ = _init_with_unix_socket

from serena.cli import top_level  # noqa: E402

sys.argv = ["serena", *sys.argv[2:]]
sys.exit(top_level())
