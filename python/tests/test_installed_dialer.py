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


def test_only_a_plain_socket_is_taken():
    import ssl

    from nfs_rs import Client, NfsError

    class Kept(socket.socket):
        detached = False

        def detach(self):
            Kept.detached = True
            return self.fileno()

    class Returning:
        def __init__(self, make):
            self.made = []
            self.make = make

        def dial(self, host, port):
            self.made.append(self.make())
            return self.made[-1]

    def subclass():
        return Kept(socket.AF_INET, socket.SOCK_STREAM)

    def wrapper():
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        return context.wrap_socket(socket.socket(), server_hostname=HOST, do_handshake_on_connect=False)

    for make in (subclass, wrapper):
        dialer = Returning(make)
        with pytest.raises(NfsError, match="plain socket.socket"):
            Client.connect(f"nfs://{HOST}/export?version=4.1", dialer=dialer)
        # Refused: the object still owns its open descriptor.
        assert dialer.made and all(made.fileno() >= 0 for made in dialer.made)
        for made in dialer.made:
            made.close()
    assert not Kept.detached


class BlockingDialer:
    """Holds every dial until released; counts the calls running at once."""

    def __init__(self):
        self.release = threading.Event()
        self.lock = threading.Condition()
        self.running = 0
        self.most = 0
        self.calls = 0

    def dial(self, host, port):
        with self.lock:
            self.calls += 1
            self.running += 1
            self.most = max(self.most, self.running)
        try:
            self.release.wait(30)
            raise ConnectionRefusedError("released")
        finally:
            with self.lock:
                self.running -= 1
                self.lock.notify_all()

    def wait_idle(self):
        with self.lock:
            assert self.lock.wait_for(lambda: self.running == 0, 30)


def test_dial_calls_past_their_deadline_are_capped():
    from nfs_rs import Client

    url = f"nfs://{HOST}/export?version=4.1"
    dialer = BlockingDialer()
    try:
        for _ in range(24):
            with pytest.raises(TimeoutError):
                Client.connect(url, dialer=dialer, connect_timeout=0.05)
        # A call past its deadline keeps its slot; later connects time out
        # waiting for one and never reach dial.
        assert dialer.calls == dialer.most == 16
    finally:
        dialer.release.set()
        dialer.wait_idle()

    refusing = RefusingDialer()
    with pytest.raises(ConnectionError):
        Client.connect(url, dialer=refusing)
    assert refusing.dialed == [(HOST, 2049)]
