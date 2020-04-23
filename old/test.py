import aiohttp
import asyncio
import time

async def connect():
    m, t = 0, 0
    mrtt, mirtt = 0, 1000000
    uri = "ws://127.0.0.1:4000"
    print("Connecting")
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(uri) as ws:
            while True:
                _start = time.time()
                await ws.send_str("H")
                await ws.receive()
                rtt = round((time.time() - _start)*10000000)/10000
                if rtt > mrtt: mrtt = rtt
                if rtt < mirtt: mirtt = rtt
                m += rtt
                t += 1
                print("RTT", rtt, "AVG", round((m/t)*1000)/1000, "MIN", mirtt, "MAX", mrtt)
                await asyncio.sleep(1/10)

loop = asyncio.get_event_loop()
loop.run_until_complete(connect())