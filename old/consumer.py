import asyncio
import time
import msgpack
import logging
import random
import zlib
import uuid

Logger = logging.getLogger("asyncio")
Logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

def createPacket(op, data=None, channel=None, _type=None, compress=True):
    packet = {
        "op": op,
    }
    if data: packet['d'] = msgpack.dumps(data)
    if channel: packet['c'] = channel

    print("data", data)
    dump = msgpack.dumps(packet)
    if compress:
        dump = zlib.compress(dump)

    packet = int.to_bytes(len(dump), 4, "big") + dump
    return packet

async def connect():
    _time = time.time()
    c = 0
    compress = True
    uri = "ws://127.0.0.1:4000"
    reader, writer = await asyncio.open_connection("127.0.0.1", "4000")

    writer.write(createPacket(1, {
        "channels": ["welcomer"],
        "state": 1,
        "compress": compress
    }))
    await writer.drain()

    while not writer.is_closing():
        print("Waiting for data size")
        data = await reader.read(4)
        size = int.from_bytes(data, "big")
        print("Waiting for main data")
        message = await reader.read(size)
        print("Done :)))")

        if compress:
            message = zlib.decompress(message)
        print("raw", message)
        message = msgpack.loads(message)

        await on_raw_message(writer, message)
    print("socket closed")

async def on_raw_message(writer, message):
    processed = []
    if type(message['d']) == list:
        for packet in message['d']:
            processed.append(str(uuid.UUID(bytes=packet['id'])))
            await on_message(packet['d'])
    else:
        packet = message['d']
        await on_message(packet['d'])
        processed.append(str(uuid.UUID(bytes=packet['id'])))
    writer.write(createPacket(3, processed))
    await writer.drain()

async def on_message(message):
    print("message", message)
    # await asyncio.sleep(0.1)

loop = asyncio.get_event_loop()
loop.run_until_complete(connect())