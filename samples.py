"""Deterministic reply recordings; indices refer to decoded outgoing commands."""
import asyncio
from collector import Collector, SFTPError
from portal import Channel

def pull(stream):
    return stream.__anext__()

async def until(predicate):
    for _ in range(200):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError('recording made no progress')

async def commands(ch, n):
    await until(lambda: len(ch.commands) >= n)
    assert len(ch.commands) == n, ch.commands

async def received(trace, ident):
    await until(lambda: any(x[:3] == ['recv', 'DATA', ident] or
                           x[:3] == ['recv', 'STATUS', ident] or
                           x[:3] == ['recv', 'HANDLE', ident] for x in trace))

async def ended(task):
    try:
        await task
    except StopAsyncIteration:
        return
    raise AssertionError('expected iterator end')

def start():
    trace = []
    ch = Channel(trace)
    return Collector(ch, trace), ch, trace

async def short():
    c, ch, trace = start()
    f = c.iter_file('/short', block=4, window=2)
    task = asyncio.create_task(pull(f))
    await commands(ch, 1)
    ch.reply(0, 'HANDLE', b'S')
    await commands(ch, 3)
    ch.reply(2, 'STATUS', (1, 'eof'))
    await received(trace, 3)
    ch.reply(1, 'DATA', b'ab')
    await commands(ch, 4)
    ch.reply(3, 'DATA', b'c')
    await commands(ch, 5)
    ch.reply(4, 'STATUS', (1, 'eof'))
    assert await task == b'abc'
    assert len(ch.commands) == 5  # Last yield does not close the handle.
    task = asyncio.create_task(pull(f))
    await commands(ch, 6)
    ch.reply(5, 'STATUS', (0, 'ok'))
    await ended(task)
    await f.aclose()  # Idempotent; emits nothing.
    await c.aclose()
    return {'chunks': ['616263'], 'trace': trace}

async def cancel():
    c, ch, trace = start()
    a = c.iter_file('/A', 4, 3)
    b = c.iter_file('/B', 4, 2)
    ta = asyncio.create_task(pull(a))
    tb = asyncio.create_task(pull(b))
    await commands(ch, 2)
    ch.reply(0, 'HANDLE', b'A')
    await commands(ch, 5)
    ch.reply(1, 'HANDLE', b'B')
    await commands(ch, 7)
    ch.reply(3, 'DATA', b'WXYZ')  # A offset 4 complete but not deliverable.
    await received(trace, 4)
    ch.reply(6, 'STATUS', (1, 'eof'))
    await received(trace, 7)
    ta.cancel()
    await until(lambda: ['stop', '/A', 'CancelledError'] in trace)
    assert not ta.done()
    ch.reply(2, 'DATA', b'a')  # Do not repair a short read after cancellation.
    await received(trace, 3)
    assert len(ch.commands) == 7 and not ta.done()
    ch.reply(5, 'DATA', b'BBBB')
    assert await tb == b'BBBB'
    tb = asyncio.create_task(pull(b))
    await commands(ch, 8)
    assert ch.commands[7][0] == 'CLOSE' and ch.commands[7][2] == '42'
    ch.reply(7, 'STATUS', (0, 'ok'))
    await ended(tb)
    assert not ta.done()  # B finishes before A's last response.
    ch.reply(4, 'STATUS', (1, 'eof'))
    await commands(ch, 9)
    ch.reply(8, 'STATUS', (4, 'close A failed'))
    try:
        await ta
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError('cancellation lost')
    assert not any(x[:2] == ['yield', '/A'] for x in trace)
    await ended(asyncio.create_task(pull(a)))
    await c.aclose()
    return {'chunks': ['42424242'], 'error': 'CancelledError', 'trace': trace}

async def error():
    c, ch, trace = start()
    f = c.iter_file('/bad', 4, 2)
    task = asyncio.create_task(pull(f))
    await commands(ch, 1)
    ch.reply(0, 'HANDLE', b'E')
    await commands(ch, 3)
    ch.reply(2, 'STATUS', (4, 'read denied'))
    await received(trace, 3)
    assert not task.done()  # Later slot failure cannot overtake earlier data.
    ch.reply(1, 'DATA', b'good')
    assert await task == b'good'
    task = asyncio.create_task(pull(f))
    await commands(ch, 4)
    await until(lambda: ['stop', '/bad', 'SFTPError'] in trace)
    assert not task.done()
    ch.reply(3, 'STATUS', (1, 'eof'))
    await commands(ch, 5)
    ch.reply(4, 'STATUS', (4, 'close denied'))
    try:
        await task
    except SFTPError as exc:
        assert (exc.code, str(exc)) == (4, 'read denied')
    else:
        raise AssertionError('read failure lost')
    await c.aclose()
    return {'chunks': ['676f6f64'], 'error': [4, 'read denied'], 'trace': trace}

async def early():
    c, ch, trace = start()
    f = c.iter_file('/early', 4, 2)
    task = asyncio.create_task(pull(f))
    await commands(ch, 1)
    ch.reply(0, 'HANDLE', b'X')
    await commands(ch, 3)
    ch.reply(1, 'DATA', b'1234')
    assert await task == b'1234'
    task = asyncio.create_task(f.aclose())
    await until(lambda: ['stop', '/early', ''] in trace)
    assert len(ch.commands) == 3 and not task.done()
    ch.reply(2, 'DATA', b'x')
    await commands(ch, 4)
    ch.reply(3, 'STATUS', (4, 'close denied'))
    try:
        await task
    except SFTPError as exc:
        assert (exc.code, str(exc)) == (4, 'close denied')
    else:
        raise AssertionError('close failure lost')
    await f.aclose()
    await c.aclose()
    return {'chunks': ['31323334'], 'error': [4, 'close denied'], 'trace': trace}

async def burst():
    c, ch, trace = start()
    a, b = c.iter_file('/idle', 4, 2), c.iter_file('/other', 4, 1)
    ta, tb = asyncio.create_task(pull(a)), asyncio.create_task(pull(b))
    await commands(ch, 2)
    ch.reply(0, 'HANDLE', b'I')
    await commands(ch, 4)
    ch.reply(1, 'HANDLE', b'O')
    await commands(ch, 5)
    ch.reply(2, 'DATA', b'abcd')
    assert await ta == b'abcd'
    # Both replies are available before the receiver resumes. A has no active pull.
    ch.reply(3, 'DATA', b'e')
    ch.reply(4, 'DATA', b'BBBB')
    await commands(ch, 6)
    assert await tb == b'BBBB'
    repair = ['send', 'READ', 6, '49', 5, 3]
    assert trace.index(repair) < trace.index(['recv', 'DATA', 5, '42424242'])
    ch.reply(5, 'DATA', b'fgh')
    await received(trace, 6)
    assert ['yield', '/idle', '65666768'] not in trace
    ta = asyncio.create_task(pull(a))
    assert await ta == b'efgh'
    await commands(ch, 7)  # Refill occurs on pull, before the buffered yield.
    ca, cb = asyncio.create_task(a.aclose()), asyncio.create_task(b.aclose())
    await commands(ch, 8)
    ch.reply(7, 'STATUS', (0, 'ok'))
    await cb
    ch.reply(6, 'STATUS', (1, 'eof'))
    await commands(ch, 9)
    ch.reply(8, 'STATUS', (0, 'ok'))
    await ca
    await c.aclose()
    return {'chunks': {'A': ['61626364', '65666768'], 'B': ['42424242']},
            'trace': trace}

async def recordings():
    return {name: await fn() for name, fn in
            [('short', short), ('cancel', cancel), ('error', error), ('early', early), ('burst', burst)]}

if __name__ == '__main__':
    import json
    print(json.dumps(asyncio.run(recordings()), ensure_ascii=False, indent=2))
