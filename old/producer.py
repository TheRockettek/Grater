import asyncio
import time
import msgpack
import logging
import zlib

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

    dump = msgpack.dumps(packet)
    if compress:
        dump = zlib.compress(dump)

    packet = int.to_bytes(len(dump), 4, "big") + dump
    return packet


async def connect():
    _time = time.time()
    c = 0
    uri = "ws://127.0.0.1:4000"
    reader, writer = await asyncio.open_connection("127.0.0.1", "4000")

    writer.write(createPacket(1, {
        "channels": ["welcomer"],
        "state": 2,
        "compress": False
    }, compress=False))
    await writer.drain()

    while not writer.is_closing():
        c += 1
        writer.write(createPacket(2, data=c, channel="welcomer", compress=False))
        print(writer.is_closing())
        await asyncio.sleep(0.1)
    print("socket closed")

async def on_message(message):
    print(message)


loop = asyncio.get_event_loop()
loop.run_until_complete(connect())