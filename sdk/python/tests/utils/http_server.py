import socket
from contextlib import closing


def free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


def check_port_open(host, port) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0
