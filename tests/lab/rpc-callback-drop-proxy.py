#!/usr/bin/env python3
import argparse
import asyncio
import struct

async def read_record(reader):
    body = bytearray()
    while True:
        marker = struct.unpack(">I", await reader.readexactly(4))[0]
        body += await reader.readexactly(marker & 0x7fffffff)
        if marker & 0x80000000:
            return bytes(body)

async def write_record(writer, body):
    writer.write(struct.pack(">I", 0x80000000 | len(body)) + body)
    await writer.drain()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--events", required=True)
    args = parser.parse_args()
    host, port = args.upstream.rsplit(":", 1)
    callback_seen = asyncio.Event()
    dropped = False
    held_calls = []
    last_callback = None

    async def handle(client_r, client_w):
        nonlocal dropped
        server_r, server_w = await asyncio.open_connection(host, int(port))

        async def server_to_client():
            nonlocal last_callback
            while True:
                record = await read_record(server_r)
                is_cb_compound = (
                    len(record) >= 24
                    and struct.unpack(">I", record[4:8])[0] == 0
                    and struct.unpack(">I", record[20:24])[0] == 1
                )
                if is_cb_compound:
                    last_callback = record
                    callback_seen.set()
                    with open(args.events, "a") as out:
                        out.write("callback-call\n")
                await write_record(client_w, record)

        async def client_to_server():
            nonlocal dropped
            while True:
                record = await read_record(client_r)
                msg_type = struct.unpack(">I", record[4:8])[0] if len(record) >= 8 else -1
                is_reply = msg_type == 1
                if is_reply and callback_seen.is_set() and not dropped:
                    dropped = True
                    with open(args.events, "a") as out:
                        out.write("callback-reply-dropped\n")
                        out.write("callback-retransmit-injected\n")
                        out.write("callback-call\n")
                    await write_record(client_w, last_callback)
                    continue
                if is_reply and callback_seen.is_set():
                    with open(args.events, "a") as out:
                        out.write("callback-reply-forwarded\n")
                    await write_record(server_w, record)
                    for held in held_calls:
                        await write_record(server_w, held)
                    held_calls.clear()
                    continue
                if msg_type == 0 and dropped:
                    held_calls.append(record)
                    with open(args.events, "a") as out:
                        out.write("fore-call-held\n")
                    continue
                await write_record(server_w, record)

        try:
            await asyncio.gather(server_to_client(), client_to_server())
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            client_w.close()
            server_w.close()

    server = await asyncio.start_server(handle, "127.0.0.1", args.listen)
    async with server:
        await server.serve_forever()

asyncio.run(main())
