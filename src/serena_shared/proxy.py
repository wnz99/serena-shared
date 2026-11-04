"""Small standard-library stdio/Unix-socket proxy for systems without Apple's nc.

Run with python -S so an idle client does not import the HTTP or async stacks.
"""

import os
import socket
import sys
import threading


def proxy(path):
    with socket.socket(socket.AF_UNIX) as connection:
        connection.connect(path)

        def upload():
            try:
                while data := os.read(0, 65536):
                    connection.sendall(data)
                connection.shutdown(socket.SHUT_WR)
            except OSError:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        # Separate blocking directions preserve full-duplex backpressure without
        # unbounded buffering. A daemon reader cannot hold exit open after peer EOF.
        threading.Thread(target=upload, daemon=True).start()
        while True:
            data = connection.recv(65536)
            if not data:
                return
            view = memoryview(data)
            while view:
                view = view[os.write(1, view):]


if __name__ == '__main__':
    proxy(sys.argv[1])
