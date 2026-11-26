from __future__ import annotations

import json
import os
import select
import socket
from typing import Any, Mapping


def make_control_pair() -> tuple[socket.socket, socket.socket]:
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    for sock in (left, right):
        os.set_inheritable(sock.fileno(), True)
    return left, right


def send_control(fd: int | socket.socket | None, message: Mapping[str, Any]) -> bool:
    if fd is None:
        return False
    sock = fd if isinstance(fd, socket.socket) else socket.socket(fileno=fd)
    owns_socket = not isinstance(fd, socket.socket)
    try:
        sock.sendall((json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8"))
        return True
    except OSError:
        return False
    finally:
        if owns_socket:
            sock.detach()


def read_control(
    fd: int | None, *, timeout: float | None = 2.0
) -> dict[str, Any] | None:
    if fd is None:
        return None
    sock = socket.socket(fileno=fd)
    try:
        ready, _, _ = select.select([sock], [], [], timeout)
        if not ready:
            return None
        chunks: list[bytes] = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break
        if not chunks:
            return None
        line = b"".join(chunks).splitlines()[0]
        result = json.loads(line.decode("utf-8"))
        return result if isinstance(result, dict) else None
    except (OSError, json.JSONDecodeError, IndexError):
        return None
    finally:
        sock.detach()


def append_event(path: str | os.PathLike[str] | None, event: Mapping[str, Any]) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
