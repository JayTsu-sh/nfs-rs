#!/usr/bin/env python3
import argparse
import asyncio
import os
import struct

CB_PROGRAM = 0x40000000
CB_SEQUENCE = 11
CB_LAYOUTRECALL = 5
OP_SEQUENCE = 53
OP_PUTFH = 22
OP_LAYOUTGET = 50
INJECT_XID = 0xC0DEC001


def u32(data, offset):
    return struct.unpack_from(">I", data, offset)[0], offset + 4


def opaque(data, offset):
    length, offset = u32(data, offset)
    end = offset + length
    return data[offset:end], (end + 3) & ~3


def word(value):
    return struct.pack(">I", value)


def hyper(value):
    return struct.pack(">Q", value)


def put_opaque(value):
    return word(len(value)) + value + bytes((-len(value)) & 3)


async def read_record(reader):
    body = bytearray()
    while True:
        marker = struct.unpack(">I", await reader.readexactly(4))[0]
        body += await reader.readexactly(marker & 0x7FFFFFFF)
        if marker & 0x80000000:
            return bytes(body)


async def write_record(writer, body, lock):
    async with lock:
        writer.write(word(0x80000000 | len(body)) + body)
        await writer.drain()


def skip_rpc_auth(data, offset):
    _, offset = u32(data, offset)
    _, offset = opaque(data, offset)
    return offset


def layoutget_fh(call):
    try:
        offset = 24
        offset = skip_rpc_auth(call, offset)
        offset = skip_rpc_auth(call, offset)
        _, offset = opaque(call, offset)  # tag
        _, offset = u32(call, offset)  # minor version
        count, offset = u32(call, offset)
        current_fh = None
        for _ in range(count):
            opcode, offset = u32(call, offset)
            if opcode == OP_SEQUENCE:
                offset += 16 + 4 * 4
            elif opcode == OP_PUTFH:
                current_fh, offset = opaque(call, offset)
            elif opcode == OP_LAYOUTGET:
                return current_fh
            else:
                return None
    except (IndexError, struct.error):
        return None
    return None


def layoutget_stateid(reply):
    try:
        offset = 12
        offset = skip_rpc_auth(reply, offset)
        accept, offset = u32(reply, offset)
        if accept != 0:
            return None
        status, offset = u32(reply, offset)
        _, offset = opaque(reply, offset)
        count, offset = u32(reply, offset)
        if status != 0 or count < 3:
            return None
        opcode, offset = u32(reply, offset)
        op_status, offset = u32(reply, offset)
        if opcode != OP_SEQUENCE or op_status != 0:
            return None
        session = reply[offset : offset + 16]
        offset += 16 + 5 * 4
        opcode, offset = u32(reply, offset)
        op_status, offset = u32(reply, offset)
        if opcode != OP_PUTFH or op_status != 0:
            return None
        opcode, offset = u32(reply, offset)
        op_status, offset = u32(reply, offset)
        if opcode != OP_LAYOUTGET or op_status != 0:
            return None
        offset += 4  # return_on_close
        stateid = reply[offset : offset + 16]
        if len(session) != 16 or len(stateid) != 16:
            return None
        return session, stateid
    except (IndexError, struct.error):
        return None


def callback_call(session, stateid, fh):
    body = b"".join(
        word(value)
        for value in [INJECT_XID, 0, 2, CB_PROGRAM, 1, 1, 0, 0, 0, 0]
    )
    body += put_opaque(b"lab-layout-recall")
    body += word(1) + word(0) + word(2)
    body += word(CB_SEQUENCE) + session
    body += word(1) + word(0) + word(0) + word(1) + word(0)
    body += word(CB_LAYOUTRECALL)
    body += word(1) + word(2) + word(0) + word(1)
    body += put_opaque(fh) + hyper(0) + hyper(0xFFFFFFFFFFFFFFFF) + stateid
    return body


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--trigger", required=True)
    parser.add_argument("--events", required=True)
    args = parser.parse_args()
    host, port = args.upstream.rsplit(":", 1)

    async def handle(client_r, client_w):
        server_r, server_w = await asyncio.open_connection(host, int(port))
        client_lock = asyncio.Lock()
        server_lock = asyncio.Lock()
        pending = {}
        layout = {"value": None}

        async def inject_when_ready():
            while not os.path.exists(args.trigger) or layout["value"] is None:
                await asyncio.sleep(0.02)
            session, stateid, fh = layout["value"]
            await write_record(client_w, callback_call(session, stateid, fh), client_lock)
            with open(args.events, "a") as out:
                out.write("layout-recall-injected\n")

        async def server_to_client():
            while True:
                record = await read_record(server_r)
                xid = struct.unpack_from(">I", record, 0)[0]
                if xid in pending:
                    parsed = layoutget_stateid(record)
                    if parsed is not None:
                        layout["value"] = (parsed[0], parsed[1], pending.pop(xid))
                        with open(args.events, "a") as out:
                            out.write("layout-captured\n")
                await write_record(client_w, record, client_lock)

        async def client_to_server():
            while True:
                record = await read_record(client_r)
                xid, message_type = struct.unpack_from(">II", record, 0)
                if message_type == 1 and xid == INJECT_XID:
                    status = struct.unpack_from(">I", record, 24)[0]
                    with open(args.events, "a") as out:
                        out.write(f"layout-recall-reply-status={status}\n")
                    continue
                if message_type == 0:
                    fh = layoutget_fh(record)
                    if fh is not None:
                        pending[xid] = fh
                await write_record(server_w, record, server_lock)

        tasks = [asyncio.create_task(inject_when_ready())]
        try:
            await asyncio.gather(server_to_client(), client_to_server(), *tasks)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            for task in tasks:
                task.cancel()
            client_w.close()
            server_w.close()

    server = await asyncio.start_server(handle, "127.0.0.1", args.listen)
    async with server:
        await server.serve_forever()


asyncio.run(main())
