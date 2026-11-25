"""Connections opened by a Python dialer, through the installed test-support wheel."""
import asyncio
import os
import socket
import struct
import threading
from urllib.parse import quote_from_bytes

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires the installed test-support wheel",
)

HOST = "nfs.example.internal"
PORTMAP_PROG, PORTMAP_PORT, GETPORT = 100000, 111, 3
MOUNT_PROG, MOUNT_PORT, EXPORT = 100005, 20048, 5


def _xdr_string(value):
    return struct.pack(">I", len(value)) + value + b"\0" * (-len(value) % 4)


def _exports_reply():
    # RFC 1813 exports: one export, one group, then the end of both lists.
    return (
        struct.pack(">I", 1) + _xdr_string(b"/data")
        + struct.pack(">I", 1) + _xdr_string(b"team") + struct.pack(">I", 0)
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


def _export_rpc(program, procedure, _request):
    if program == PORTMAP_PROG and procedure == GETPORT:
        return struct.pack(">I", MOUNT_PORT)
    if program == MOUNT_PROG and procedure == EXPORT:
        return _exports_reply()
    return b""


def _serve(sock, reply_body):
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
            payload = reply_body(prog, proc, record)
            # Accepted reply, AUTH_NONE verifier, SUCCESS.
            reply = struct.pack(">6I", xid, 1, 0, 0, 0, 0) + payload
            sock.sendall(struct.pack(">I", 0x80000000 | len(reply)) + reply)


class ServingDialer:
    """Hands out one end of a socket pair and serves RPC on the other."""

    def __init__(self, reply_body=_export_rpc):
        self.dialed = []
        self.threads = []
        self.reply_body = reply_body

    def dial(self, host, port):
        self.dialed.append((host, port, threading.current_thread() is threading.main_thread()))
        client, server = socket.socketpair()
        thread = threading.Thread(target=_serve, args=(server, self.reply_body), daemon=True)
        thread.start()
        self.threads.append(thread)
        return client

    def join(self):
        for thread in self.threads:
            thread.join(timeout=10)
            assert not thread.is_alive(), "the client must close every dialed connection"


def _v3_mount_reply(program, procedure):
    if program == PORTMAP_PROG:  # no optional NFSACL service
        return struct.pack(">I", 0)
    if (program, procedure) in ((100227, 0), (MOUNT_PROG, 3)):
        return b""
    if (program, procedure) == (MOUNT_PROG, 1):
        return struct.pack(">II4sII", 0, 4, b"root", 1, 1)
    assert (program, procedure) == (100003, 19)
    return struct.pack(">9IQ3I", 0, 0, 4096, 4096, 512, 4096, 4096, 512, 512, 1 << 42, 0, 1, 0)


def test_v3_paged_names_drive_rename_and_remove_without_collisions():
    from nfs_rs import Client

    names = (b"\xff", b"\xfe", b"~nfs-rs-raw:FF")
    renamed = b"moved\xff"
    verifier = b"page-v3!"
    operations = []

    def rpc_reply(program, procedure, request):
        if program == 100003:
            if procedure == 16:
                cookie, received_verifier, _ = struct.unpack(">Q8sI", request[-20:])
                assert received_verifier == (verifier if cookie else bytes(8))
                operations.append(("readdir", cookie))
                return (
                    struct.pack(">II8sIQ", 0, 0, verifier, 1, cookie + 1)
                    + _xdr_string(names[cookie])
                    + struct.pack(">QII", cookie + 1, 0, cookie + 1 == len(names))
                )
            directory = _xdr_string(b"root")
            if procedure == 14:
                assert request.endswith(directory + _xdr_string(names[0]) + directory + _xdr_string(renamed))
                operations.append(("rename", renamed))
                return bytes(20)
            if procedure == 12:
                assert request.endswith(directory + _xdr_string(renamed))
                operations.append(("remove", renamed))
                return bytes(12)
        return _v3_mount_reply(program, procedure)

    dialer = ServingDialer(rpc_reply)
    url = f"nfs://{HOST}:{MOUNT_PORT}/data?version=3&mountport={MOUNT_PORT}"
    with Client.connect(url, dialer=dialer) as client:
        entries = client.listdir(b"")
        assert tuple(entries) == names
        client.rename(entries[0], renamed)
        client.remove(renamed)
    del client
    dialer.join()
    assert operations == [("readdir", 0), ("readdir", 1), ("readdir", 2), ("rename", renamed), ("remove", renamed)]


def test_listdir_can_collect_names_from_thousands_of_small_pages():
    from nfs_rs import AsyncClient, Client

    entries = [f"entry-{i:04}".encode() + b"\xff" for i in range(5_500)]
    verifier = b"listing!"

    def directory_rpc(program, procedure, request):
        if (program, procedure) != (100003, 16):
            return _v3_mount_reply(program, procedure)
        cookie, received_verifier, _ = struct.unpack(">Q8sI", request[-20:])
        assert received_verifier == (verifier if cookie else bytes(8))
        name = entries[cookie]
        return (
            struct.pack(">II8sIQI", 0, 0, verifier, 1, cookie + 1, len(name))
            + name + bytes((-len(name)) % 4)
            + struct.pack(">QII", cookie + 1, 0, cookie + 1 == len(entries))
        )

    dialer = ServingDialer(directory_rpc)
    url = f"nfs://{HOST}:{MOUNT_PORT}/data?version=3&mountport={MOUNT_PORT}&readdir-buffer=512"
    with Client.connect(url, dialer=dialer) as client:
        views = client.listdir(b"")
        assert views == entries and all(view.readonly for view in views)
    # NFSv3 UMNT is advisory. The final mount reference owns transport shutdown.
    del client
    assert views == entries

    async def scenario():
        async with await AsyncClient.connect(url, dialer=dialer) as client:
            assert await client.listdir(b"") == entries

    asyncio.run(scenario())
    dialer.join()


class RefusingDialer:
    def __init__(self):
        self.dialed = []

    def dial(self, host, port):
        self.dialed.append((host, port))
        raise ConnectionRefusedError("refused by the test dialer")


def _expected_exports():
    from nfs_rs import ExportEntry

    return (ExportEntry(b"/data", (b"team",)),)


@pytest.mark.parametrize("path", [b"/", b"/raw/\xff/%2F/", b"/" + b"a" * 1023])
def test_export_bytes_round_trip_through_v3_mount_and_umount(path):
    from nfs_rs import AsyncClient, Client, ExportEntry, list_exports, list_exports_async

    group = b"team-\xfe"
    exports = struct.pack(">I", 1) + _xdr_string(path)
    exports += struct.pack(">I", 1) + _xdr_string(group) + struct.pack(">2I", 0, 0)
    mount_calls = []

    def mount_rpc(program, procedure, request):
        if (program, procedure) == (MOUNT_PROG, EXPORT):
            return exports
        if program == MOUNT_PROG and procedure in (1, 3):
            expected = _xdr_string(path)
            mount_calls.append((procedure, request[-len(expected):]))
            return struct.pack(">II4sII", 0, 4, b"root", 1, 1) if procedure == 1 else b""
        if program == PORTMAP_PROG:
            return struct.pack(">I", 0)
        if (program, procedure) == (100227, 0):
            return b""
        assert (program, procedure) == (100003, 19)
        return struct.pack(">9IQ3I", 0, 0, 4096, 4096, 512, 4096, 4096, 512, 512, 1 << 42, 0, 1, 0)

    dialer = ServingDialer(mount_rpc)
    host = f"nfs://{HOST}:{MOUNT_PORT}"
    options = {"dialer": dialer, "mount_port": MOUNT_PORT}

    def export_url(export):
        return f"{host}{quote_from_bytes(export.path, safe='/')}?version=3"

    (export,) = list_exports(host, **options)
    assert export == ExportEntry(path, (group,))
    with Client.connect(export_url(export), **options) as client:
        assert client.version == "3"
    del client

    async def scenario():
        (export,) = await list_exports_async(host, **options)
        assert export == ExportEntry(path, (group,))
        async with await AsyncClient.connect(export_url(export), **options) as client:
            assert client.version == "3"

    asyncio.run(scenario())
    dialer.join()
    assert mount_calls == [(op, _xdr_string(path)) for op in (1, 3, 1, 3)]


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
