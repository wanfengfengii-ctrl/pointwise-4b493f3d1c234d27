"""Frozen behavioral checks. Extend verification elsewhere; preserve these assertions."""
import asyncio
import hashlib
import json
from collector import Collector
from portal import Channel
from samples import recordings

GOLDEN = {
    'burst': 'a79e8e210e2d6beb5b43d29ff97ec2db9196b1be45a69e693075f84bd5e26163',
    'short': '0f5a846da5e9f8e4dd8b96833107c7bdff9d757126f63a7b334cb178ffd09cef',
    'cancel': 'ca7d70a03ba5c21a92b405aa6af4be6e9f5313aa6f050ab9de551c21b77f34e3',
    'error': '9e95ea14a125f192d5fbb71c92f73546333f51fd0bd833ff3956f6a6ab943419',
    'early': 'f7d3b84193102ddda8f829589710e4983dc2b3a09a9beb268eadfb5c5cd18a89',
}
SOURCES = {
    '/notice.txt': '河道公告\n水位：12.30\n'.encode(),
    '/empty.txt': b'',
    '/measurements.txt': b'site,level\nA,12.30\nB,0.00\n',
}

async def byte_reference(block, window):
    trace = []
    ch = Channel(trace)
    c = Collector(ch, trace)
    handles = {}
    served = set()
    async def server():
        while True:
            indices = [i for i in range(len(ch.commands)) if i not in served]
            if not indices:
                await asyncio.sleep(0)
                continue
            i = indices[-1]  # Deliberately reverse ready responses.
            served.add(i)
            cmd = ch.commands[i]
            if cmd[0] == 'OPEN':
                handle = cmd[2].encode()
                handles[handle.hex()] = SOURCES[cmd[2]]
                ch.reply(i, 'HANDLE', handle)
            elif cmd[0] == 'READ':
                data, off, count = handles[cmd[2]], cmd[3], cmd[4]
                if off >= len(data):
                    ch.reply(i, 'STATUS', (1, 'eof'))
                else:
                    n = min(count, 1 + off % 3)
                    ch.reply(i, 'DATA', data[off:off+n])
            else:
                ch.reply(i, 'STATUS', (0, 'ok'))
            await asyncio.sleep(0)
    async def consume(path):
        chunks = []
        async for item in c.iter_file(path, block, window):
            chunks.append(item)
        data = SOURCES[path]
        assert chunks == [data[i:i+block] for i in range(0, len(data), block)]
        return chunks
    peer = asyncio.create_task(server())
    try:
        chunks = await asyncio.wait_for(
            asyncio.gather(*(consume(path) for path in SOURCES)), timeout=5)
        # Independently count network requests by handle, rather than read internals.
        active = {}
        outstanding = {}
        for item in trace:
            if item[:2] == ['send', 'READ']:
                ident, handle = item[2], item[3]
                outstanding[ident] = handle
                active[handle] = active.get(handle, 0) + 1
                assert active[handle] <= window
            elif item[0] == 'recv' and item[2] in outstanding:
                handle = outstanding.pop(item[2])
                active[handle] -= 1
            elif item[:2] == ['send', 'CLOSE']:
                assert active.get(item[3], 0) == 0
        assert not outstanding
        await c.aclose()
        return sum(map(len, chunks))
    finally:
        peer.cancel()
        try:
            await peer
        except asyncio.CancelledError:
            pass

async def verify():
    result = await recordings()
    for name, value in result.items():
        payload = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
        assert hashlib.sha256(payload).hexdigest() == GOLDEN[name], (name, value)
    cases = 0
    for block in (1, 3, 8):
        for window in (1, 2, 3):
            await byte_reference(block, window)
            cases += 1
    print('BASELINE_OK frozen=5 byte_reference=' + str(cases))

if __name__ == '__main__':
    asyncio.run(verify())
