"""Test the lightweight client at real OS pipe/socket boundaries."""

import signal
import socket
import subprocess
import sys
import threading
import tempfile
from pathlib import Path

import pytest

from serena_shared import proxy


def commands(path):
    yield [sys.executable, '-S', str(Path(proxy.__file__)), str(path)]
    if sys.platform == 'darwin':
        yield ['/usr/bin/nc', '-U', str(path)]


@pytest.fixture
def tmp_path():
    with tempfile.TemporaryDirectory(prefix='sp-', dir='/tmp') as directory:
        yield Path(directory)


def test_stdio_roundtrip_and_eof(tmp_path):
    path = tmp_path / 's'
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(path))
        server.listen()
        for command in commands(path):
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            peer, _ = server.accept()
            try:
                process.stdin.write(b'{"hello":"world"}\n')
                process.stdin.flush()
                peer.settimeout(5)
                assert peer.recv(100) == b'{"hello":"world"}\n'
                peer.sendall(b'{"result":true}\n')
                assert process.stdout.readline() == b'{"result":true}\n'
                process.stdin.close()
                assert peer.recv(1) == b''
                peer.close()
                assert process.wait(timeout=5) == 0
            finally:
                peer.close()
                if process.poll() is None:
                    process.kill()
                process.wait()


def test_proxy_full_duplex_backpressure_and_peer_close(tmp_path):
    path = tmp_path / 's'
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(path))
        server.listen()
        for command in commands(path):
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            peer, _ = server.accept()
            payload = b'x' * 2_000_000
            def upload():
                process.stdin.write(payload)
                process.stdin.close()
            thread = threading.Thread(target=upload, daemon=True)
            thread.start()
            def echo():
                with peer:
                    while data := peer.recv(65536):
                        peer.sendall(data)
            worker = threading.Thread(target=echo, daemon=True)
            worker.start()
            try:
                assert process.stdout.read() == payload
                assert process.wait(timeout=5) == 0
                thread.join(timeout=5)
                worker.join(timeout=5)
                assert not thread.is_alive() and not worker.is_alive()
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()


def test_proxy_signal_exit(tmp_path):
    path = tmp_path / 's'
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(path))
        server.listen()
        for command in commands(path):
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            peer, _ = server.accept()
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=5)
            peer.settimeout(5)
            assert peer.recv(1) == b''
            peer.close()


def test_proxy_does_not_import_http_stack():
    source = Path(proxy.__file__).read_text()
    assert 'import anyio' not in source and 'import httpx' not in source
