"""Connections opened by a Python dialer, through the installed test-support wheel."""
import asyncio
import os
import socket
import struct
import threading

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires the installed test-support wheel",
)

HOST = "nfs.example.internal"
PORTMAP_PROG, PORTMAP_PORT, GETPORT = 100000, 111, 3
MOUNT_PROG, MOUNT_PORT, EXPORT = 100005, 20048, 5


def _xdr_string(value):
    data = value.encode()
    return struct.pack(">I", len(data)) + data + b"\0" * (-len(data) % 4)


def _exports_reply():
    # RFC 1813 exports: one export, one group, then the end of both lists.
    return (
        struct.pack(">I", 1) + _xdr_string("/data")
        + struct.pack(">I", 1) + _xdr_string("team") + struct.pack(">I", 0)
        + struct.pack(">I", 0)
    )


def _read_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def _serve(sock):
    """Answers portmapper and MOUNT calls (RFC 5531 record marking) until EOF."""
    with sock:
        while True:
            marker = _read_exact(sock, 4)
            if marker is None:
                return
            record = _read_exact(sock, struct.unpack(">I", marker)[0] & 0x7FFFFFFF)
            if record is None:
                return
            xid, _, _, prog, _, proc = struct.unpack(">6I", record[:24])
            if prog == PORTMAP_PROG and proc == GETPORT:
                payload = struct.pack(">I", MOUNT_PORT)
            elif prog == MOUNT_PROG and proc == EXPORT:
                payload = _exports_reply()
            else:
                payload = b""
            # Accepted reply, AUTH_NONE verifier, SUCCESS.
            reply = struct.pack(">6I", xid, 1, 0, 0, 0, 0) + payload
            sock.sendall(struct.pack(">I", 0x80000000 | len(reply)) + reply)


class ServingDialer:
    """Hands out one end of a socket pair and serves RPC on the other."""

    def __init__(self):
        self.dialed = []
        self.threads = []

    def dial(self, host, port):
        self.dialed.append((host, port, threading.current_thread() is threading.main_thread()))
        client, server = socket.socketpair()
        thread = threading.Thread(target=_serve, args=(server,), daemon=True)
        thread.start()
        self.threads.append(thread)
        return client

    def join(self):
        for thread in self.threads:
            thread.join(timeout=10)
            assert not thread.is_alive(), "the client must close every dialed connection"


class RefusingDialer:
    def __init__(self):
        self.dialed = []

    def dial(self, host, port):
        self.dialed.append((host, port))
        raise ConnectionRefusedError("refused by the test dialer")


def _expected_exports():
    from nfs_rs import ExportEntry

    return (ExportEntry("/data", ("team",)),)


def test_list_exports_dials_every_connection_through_the_dialer():
    from nfs_rs import Dialer, list_exports

    dialer = ServingDialer()
    assert isinstance(dialer, Dialer)

    assert list_exports(HOST, dialer=dialer) == _expected_exports()

    dialer.join()
    assert dialer.dialed == [(HOST, PORTMAP_PORT, False), (HOST, MOUNT_PORT, False)]


def test_async_list_exports_dials_through_the_dialer():
    from nfs_rs import list_exports_async

    dialer = ServingDialer()

    exports = asyncio.run(list_exports_async(HOST, dialer=dialer, mount_port=MOUNT_PORT))

    assert exports == _expected_exports()
    dialer.join()
    assert dialer.dialed == [(HOST, MOUNT_PORT, False)]


def test_dialer_errors_keep_their_kind():
    from nfs_rs import AsyncClient, Client, NfsError

    url = f"nfs://{HOST}/export?version=4.1"
    for connect in (
        lambda dialer: Client.connect(url, dialer=dialer),
        lambda dialer: asyncio.run(AsyncClient.connect(url, dialer=dialer)),
    ):
        dialer = RefusingDialer()
        with pytest.raises(ConnectionError) as raised:
            connect(dialer)
        assert isinstance(raised.value, NfsError)
        assert "refused by the test dialer" in str(raised.value)
        assert dialer.dialed == [(HOST, 2049)]


def test_dial_must_return_a_connected_stream_socket():
    from nfs_rs import NfsError, list_exports

    class NotASocket:
        def dial(self, host, port):
            return object()

    class Datagram:
        def __init__(self):
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        def dial(self, host, port):
            return self.socket

    for dialer, message in ((NotASocket(), "socket.socket"), (Datagram(), "SOCK_STREAM")):
        with pytest.raises(NfsError, match=message):
            list_exports(HOST, dialer=dialer)
    # A refused socket is left with its owner, open.
    assert dialer.socket.fileno() >= 0
    dialer.socket.close()


def test_dialer_needs_a_dial_method():
    from nfs_rs import AsyncClient, Client, list_exports

    for dialer in (object(), type("NotCallable", (), {"dial": 1})()):
        with pytest.raises(TypeError, match="callable dial"):
            Client.connect(f"nfs://{HOST}/export", dialer=dialer)
        with pytest.raises(TypeError, match="callable dial"):
            asyncio.run(AsyncClient.connect(f"nfs://{HOST}/export", dialer=dialer))
        with pytest.raises(TypeError, match="callable dial"):
            list_exports(HOST, dialer=dialer)


def test_retained_delegations_are_refused_with_a_dialer():
    from nfs_rs import AsyncClient, Client

    url = f"nfs://{HOST}/export?version=4.0"
    dialer = RefusingDialer()
    with pytest.raises(ValueError, match="retain-delegations"):
        Client.connect(url, dialer=dialer, retain_delegations=True)
    with pytest.raises(ValueError, match="retain-delegations"):
        asyncio.run(AsyncClient.connect(url, dialer=dialer, retain_delegations=True))
    assert dialer.dialed == []
