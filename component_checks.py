"""Independent component checks: no Collector, real broker or real channel.

FileReader is driven by stub brokers whose routes expose only the published
port; RequestBroker is driven by a stub channel implementing only send/recv.
The scenarios exercise out-of-order replies, idle short-read repair, a later
error behind earlier data, and cancellation cleanup, and confirm each reader
touches only its own state.
"""
import ast
import asyncio
import dataclasses
import os

from codec import decode, response
from wire import Reply

ROOT = os.path.dirname(os.path.abspath(__file__))


def source(name):
    with open(os.path.join(ROOT, name), encoding='utf-8') as handle:
        return handle.read()


async def until(predicate, limit=500):
    for _ in range(limit):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError('stub made no progress')


async def settle():
    for _ in range(3):
        await asyncio.sleep(0)


# -- reader-side stubs: only the published route port -------------------------

class StubRoute:
    """Minimal route used by FileReader tests; it is not a broker."""

    def __init__(self):
        self.requests = []       # ordered [{'op','tag','args'}]
        self.outstanding = []    # requests whose reply has not crossed
        self.broken = None
        self.handler = None
        self.notify_owner = None
        self._idle = asyncio.Event()
        self._idle.set()

    # published port
    def submit_open(self, path):
        self._record('OPEN', None, (path,))

    def submit_read(self, handle, offset, length, tag):
        self._record('READ', tag, (handle, offset, length))

    def submit_close(self, handle):
        self._record('CLOSE', None, (handle,))

    async def wait_idle(self):
        while self.outstanding:
            if self.broken is not None:
                raise self.broken
            self._idle.clear()
            await self._idle.wait()
        if self.broken is not None:
            raise self.broken

    # test side
    def _record(self, op, tag, args):
        entry = {'op': op, 'tag': tag, 'args': args}
        self.requests.append(entry)
        self.outstanding.append(entry)
        self._idle.clear()

    def _consume(self, op, tag):
        entry = next((e for e in self.outstanding
                      if e['op'] == op and e['tag'] == tag), None)
        if entry is None:
            raise AssertionError('no outstanding %s tag=%r' % (op, tag))
        self.outstanding.remove(entry)
        return entry

    def deliver(self, op, kind, tag, payload=None, code=None, message=None):
        entry = self._consume(op, tag)
        self.handler(Reply(op, kind, entry['tag'], payload, code, message))
        self.notify_owner()
        if not self.outstanding:
            self._idle.set()

    def handle_reply(self, payload):
        self.deliver('OPEN', 'HANDLE', None, payload=payload)

    def data(self, tag, payload):
        self.deliver('READ', 'DATA', tag, payload=payload)

    def status(self, tag, code, message='x'):
        self.deliver('READ', 'STATUS', tag, code=code, message=message)

    def close_reply(self, code, message='x'):
        self.deliver('CLOSE', 'STATUS', None, code=code, message=message)

    def ops(self):
        return [e['op'] for e in self.requests]

    def reads(self):
        return [e['args'] for e in self.requests if e['op'] == 'READ']


class RouteView:
    """Passed to FileReader; any access outside the published port is refused."""

    __slots__ = ('_impl', 'foreign')

    def __init__(self, impl):
        object.__setattr__(self, '_impl', impl)
        object.__setattr__(self, 'foreign', set())

    def submit_open(self, path):
        self._impl.submit_open(path)

    def submit_read(self, handle, offset, length, tag):
        self._impl.submit_read(handle, offset, length, tag)

    def submit_close(self, handle):
        self._impl.submit_close(handle)

    @property
    def outstanding(self):
        return len(self._impl.outstanding)

    @property
    def broken(self):
        return self._impl.broken

    async def wait_idle(self):
        return await self._impl.wait_idle()

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        self.foreign.add(name)
        raise AttributeError(name)


class StubBroker:
    """Only open_route; proves FileReader needs no real broker or collector."""

    def __init__(self):
        self.routes = []
        self.views = []

    def open_route(self, handler, notify):
        impl = StubRoute()
        impl.handler = handler
        impl.notify_owner = notify
        self.routes.append(impl)
        view = RouteView(impl)
        self.views.append(view)
        return view


def make_reader(path='/x', block=4, window=2, trace=None, broker=None):
    from reader import FileReader
    broker = StubBroker() if broker is None else broker
    return FileReader(broker, path, block, window, [] if trace is None else trace), broker


def view(broker, index=0):
    return broker.routes[index]


def no_foreign(broker):
    # FileReader was handed only a RouteView; any other attribute access on the
    # route is recorded and would fail here.
    for route_view in broker.views:
        assert not route_view.foreign, route_view.foreign
    return True


async def scenario_reader_out_of_order():
    trace = []
    r, broker = make_reader('/order', 4, 2, trace)
    route = view(broker)
    task = asyncio.create_task(r.__anext__())
    await until(lambda: len(route.requests) == 1)
    assert route.requests[0] == {'op': 'OPEN', 'tag': None, 'args': ('/order',)}
    route.handle_reply(b'H')
    await until(lambda: len(route.requests) == 3)
    assert route.reads() == [(b'H', 0, 4), (b'H', 4, 4)]
    # The later EOF must not overtake the earlier, unfinished block.
    route.status(4, 1, 'eof')
    await settle()
    assert not task.done() and not any(x[0] == 'yield' for x in trace)
    # A short DATA is repaired immediately, while the pull is still waiting.
    route.data(0, b'ab')
    await until(lambda: len(route.requests) == 4)
    assert route.reads()[-1] == (b'H', 2, 2)
    assert not task.done()
    route.data(0, b'cd')                  # repair reply, same slot
    assert await task == b'abcd'
    # The known EOF forbids further refill; the next pull cleans up and ends.
    task = asyncio.create_task(r.__anext__())
    await until(lambda: route.ops()[-1] == 'CLOSE')
    assert route.ops().count('READ') == 3
    assert len(route.outstanding) == 1     # CLOSE reply not yet back
    route.close_reply(0, 'ok')
    try:
        await task
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError('expected end')
    assert trace[-2:] == [['stop', '/order', ''], ['closed', '/order']]
    await r.aclose()
    assert route.ops()[-1] == 'CLOSE'
    assert no_foreign(broker)
    return len(route.requests)


async def scenario_reader_idle_repair():
    trace = []
    r, broker = make_reader('/idle', 4, 2, trace)
    route = view(broker)
    task = asyncio.create_task(r.__anext__())
    await until(lambda: len(route.requests) == 1)
    route.handle_reply(b'I')
    await until(lambda: len(route.requests) == 3)
    route.data(0, b'abcd')
    assert await task == b'abcd'
    # No pull is active now. A short DATA on the buffered slot must still send
    # the remaining interval at once, before any other reply is consumed.
    route.data(4, b'e')
    assert route.reads()[-1] == (b'I', 5, 3)
    assert len(route.requests) == 4
    assert ['yield', '/idle', '65666768'] not in trace
    route.data(4, b'fgh')
    task = asyncio.create_task(r.__anext__())
    assert await task == b'efgh'
    # The refill triggered by the pull leaves READ tag 8 in flight.
    await until(lambda: route.reads()[-1] == (b'I', 8, 4))
    closing = asyncio.create_task(r.aclose())
    await settle()
    assert route.ops()[-1] == 'READ' and not closing.done()
    route.data(8, b'Z')                   # stopping: short DATA is not repaired
    await until(lambda: route.ops()[-1] == 'CLOSE')
    assert route.reads()[-1] == (b'I', 8, 4)
    route.close_reply(0, 'ok')
    await closing
    await r.aclose()
    assert no_foreign(broker)
    return len(route.requests)


async def scenario_reader_later_error():
    trace = []
    r, broker = make_reader('/bad', 4, 2, trace)
    route = view(broker)
    task = asyncio.create_task(r.__anext__())
    await until(lambda: len(route.requests) == 1)
    route.handle_reply(b'E')
    await until(lambda: len(route.requests) == 3)
    # A later-position error cannot overtake the earlier block.
    route.status(4, 4, 'read denied')
    await settle()
    assert not task.done()
    route.data(0, b'good')
    assert await task == b'good'
    # The next pull first opens a newly freed window slot, then raises the
    # pending error; that command must not be optimized away.
    task = asyncio.create_task(r.__anext__())
    await until(lambda: len(route.requests) == 4)
    assert route.reads()[-1] == (b'E', 8, 4)
    await until(lambda: ['stop', '/bad', 'SFTPError'] in trace)
    assert not task.done()
    route.data(8, b'zz')                  # dropped during cleanup
    assert route.reads()[-1] == (b'E', 8, 4)
    await until(lambda: len(route.requests) == 5)
    assert route.ops()[-1] == 'CLOSE'
    route.close_reply(4, 'close denied')
    try:
        await task
    except Exception as exc:
        assert (type(exc).__name__, exc.code, str(exc)) == (
            'SFTPError', 4, 'read denied')
    else:
        raise AssertionError('later error overtook earlier data')
    await r.aclose()
    assert no_foreign(broker)
    return len(route.requests)


async def scenario_reader_cancel():
    trace = []
    # One shared stub broker: both readers must stay on their own route.
    broker = StubBroker()
    r, _ = make_reader('/A', 4, 2, trace, broker)
    peer, _ = make_reader('/B', 4, 2, [], broker)
    route, other = view(broker, 0), view(broker, 1)
    task = asyncio.create_task(r.__anext__())
    peer_task = asyncio.create_task(peer.__anext__())
    await until(lambda: len(route.requests) == 1 and len(other.requests) == 1)
    route.handle_reply(b'A')
    other.handle_reply(b'B')
    await until(lambda: len(route.requests) == 3 and len(other.requests) == 3)
    route.data(4, b'WXYZ')                # buffered, not yet deliverable
    await settle()
    task.cancel()
    await until(lambda: ['stop', '/A', 'CancelledError'] in trace)
    assert not task.done()
    # WXYZ arrived, so it is not outstanding; only READ tag 0 blocks CLOSE.
    assert len(route.outstanding) == 1
    # Meanwhile the other file still reads, yields and closes independently.
    other.data(0, b'BBBB')
    assert await peer_task == b'BBBB'
    peer_task = asyncio.create_task(peer.__anext__())
    await until(lambda: len(other.requests) == 4)
    assert other.ops()[-1] == 'READ'
    other.data(4, b'BBBB')
    assert await peer_task == b'BBBB'
    before = len(route.requests)
    route.data(0, b'ab')                  # late short DATA after cancellation
    await settle()
    assert route.reads()[-1] == (b'A', 4, 4)   # no repair READ for offset 2
    assert len(route.requests) == before + 1   # exactly the CLOSE attempt
    assert route.ops()[-1] == 'CLOSE'
    assert not task.done()
    route.close_reply(4, 'close A failed')
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError('cancellation lost')
    assert not any(x[0] == 'yield' for x in trace)
    assert trace[-1] == ['closed', '/A']
    await r.aclose()
    # The other file is still closable after A's cancellation.
    closing = asyncio.create_task(peer.aclose())
    assert other.ops()[-1] == 'READ' and not closing.done()
    other.status(8, 1, 'eof')
    await until(lambda: other.ops()[-1] == 'CLOSE')
    other.close_reply(0, 'ok')
    await closing
    await peer.aclose()
    assert no_foreign(broker)
    return len(route.requests)


async def scenario_reader_close_failure():
    trace = []
    r, broker = make_reader('/close', 4, 2, trace)
    route = view(broker)
    task = asyncio.create_task(r.__anext__())
    await until(lambda: len(route.requests) == 1)
    route.handle_reply(b'X')
    await until(lambda: len(route.requests) == 3)
    route.data(0, b'1234')
    assert await task == b'1234'
    closing = asyncio.create_task(r.aclose())
    await until(lambda: ['stop', '/close', ''] in trace)
    assert route.ops()[-1] != 'CLOSE'
    route.data(4, b'x')                   # short, but stopping: no repair
    await until(lambda: route.ops()[-1] == 'CLOSE')
    assert all(entry['args'][1] != 5 for entry in route.requests
               if entry['op'] == 'READ')
    route.close_reply(7, 'close denied')
    try:
        await closing
    except Exception as exc:
        assert (type(exc).__name__, exc.code, str(exc)) == (
            'SFTPError', 7, 'close denied')
    else:
        raise AssertionError('close failure lost')
    await r.aclose()                      # idempotent; old error not repeated
    assert trace.count(['closed', '/close']) == 1
    assert no_foreign(broker)
    return len(route.requests)


# -- broker-side stub: only send/recv ----------------------------------------

class StubChannel:
    __slots__ = ('sent', 'incoming', 'trace')

    def __init__(self, trace):
        self.sent = []
        self.incoming = asyncio.Queue()
        self.trace = trace

    def send(self, packet):
        self.sent.append(packet)
        self.trace.append(['send'] + decode(packet))

    async def recv(self):
        return await self.incoming.get()

    def reply(self, index, kind, value):
        ident = decode(self.sent[index])[1]
        self.incoming.put_nowait(response(ident, kind, value))


async def scenario_broker():
    from broker import RequestBroker
    trace = []
    channel = StubChannel(trace)
    broker = RequestBroker(channel, trace)
    seen_a, seen_b = [], []
    notify = {'a': 0, 'b': 0}
    ra = broker.open_route(lambda reply: seen_a.append((reply, ra.outstanding)),
                           lambda: notify.__setitem__('a', notify['a'] + 1))
    rb = broker.open_route(lambda reply: seen_b.append((reply, rb.outstanding)),
                           lambda: notify.__setitem__('b', notify['b'] + 1))
    tag = ('owner-a', 0)
    ra.submit_open('/a')
    rb.submit_open('/b')
    ra.submit_read(b'H', 0xffffffff, 3, tag)  # offset handed through opaquely
    ra.submit_close(b'H')
    assert [decode(p)[0] for p in channel.sent] == ['OPEN', 'OPEN', 'READ', 'CLOSE']
    assert [decode(p)[1] for p in channel.sent] == [1, 2, 3, 4]
    assert decode(channel.sent[2])[3:] == [0xffffffff, 3]
    assert ra.outstanding == 3 and rb.outstanding == 1
    # Reverse order across two routes.
    channel.reply(1, 'HANDLE', b'B')
    await until(lambda: len(seen_b) == 1)
    assert seen_b[0][0] == Reply('OPEN', 'HANDLE', None, payload=b'B')
    assert seen_b[0][1] == 0 and rb.outstanding == 0
    assert ra.outstanding == 3 and notify == {'a': 0, 'b': 1}
    channel.reply(3, 'STATUS', (0, 'ok'))
    await until(lambda: len(seen_a) == 1)
    assert seen_a[0][0].operation == 'CLOSE' and seen_a[0][1] == 2
    assert seen_b[0][1] == 0                 # B's later replies never route to A
    channel.reply(0, 'HANDLE', b'A')
    await until(lambda: len(seen_a) == 2)
    assert seen_a[1][0].payload == b'A' and seen_a[1][1] == 1
    channel.reply(2, 'DATA', b'xyz')
    await until(lambda: len(seen_a) == 3)
    result, remaining = seen_a[2]
    assert result == Reply('READ', 'DATA', tag, payload=b'xyz')
    assert result.tag is tag                 # correlation is transferred intact
    assert remaining == 0 and ra.outstanding == 0
    for reply in (seen_a[0][0], seen_a[1][0], seen_a[2][0]):
        with_dataclass = dataclasses.is_dataclass(reply)
        assert with_dataclass
        try:
            reply.payload = b'changed'
        except dataclasses.FrozenInstanceError:
            pass
        else:
            raise AssertionError('routed result is mutable')
    assert [x[:2] for x in trace] == [
        ['send', 'OPEN'], ['send', 'OPEN'], ['send', 'READ'], ['send', 'CLOSE'],
        ['recv', 'HANDLE'], ['recv', 'STATUS'], ['recv', 'HANDLE'], ['recv', 'DATA']]
    assert notify == {'a': 3, 'b': 1}
    await broker.aclose()
    # A fresh stub channel object is the only dependency the broker needed.
    assert isinstance(channel, StubChannel)
    return len(channel.sent)


# -- dependency direction, assembly-only Collector, stand-in capability -----

def _imports(path):
    tree = ast.parse(source(path))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name.split('.')[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module.split('.')[0])
    return found


def _methods(path, cls):
    tree = ast.parse(source(path))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == cls:
            return {n.name for n in node.body if isinstance(n, (
                ast.FunctionDef, ast.AsyncFunctionDef))}
    raise AssertionError('class missing: ' + cls)


def verify_layout():
    from collector import Collector
    from broker import RequestBroker
    from reader import FileReader
    import broker as broker_module
    import reader as reader_module
    # Dependency direction: no broker->reader/collector, no reader->broker/collector.
    assert 'reader' not in _imports('broker.py')
    assert 'collector' not in _imports('broker.py')
    assert 'broker' not in _imports('reader.py')
    assert 'collector' not in _imports('reader.py')
    assert _imports('wire.py') <= {'dataclasses', 'typing'}
    # Assembly only: no legacy business chain, inheritance or request table.
    assert Collector.__bases__ == (object,)
    assert RequestBroker.__bases__ == (object,)
    assert FileReader.__bases__ == (object,)
    assert _methods('collector.py', 'Collector') == {
        '__init__', 'iter_file', 'aclose'}
    for name in ('_receive', '_next', '_fill', '_finish', '_send', '_wait',
                 '_File', 'pending', 'serial', 'pump'):
        assert not hasattr(Collector, name), name
    for name in ('_send', 'pending', '_receive'):
        assert not hasattr(FileReader, name), name
    assert 'print(' not in source('collector.py')
    assert '_File' not in source('collector.py')
    # The reader's connection surface is the port only.
    assert 'channel' not in reader_module.__dict__
    assert 'decode' not in reader_module.__dict__
    assert 'command' not in reader_module.__dict__
    # The broker never exposes per-file semantics or iteration.
    for name in ('iter_file', 'slots', 'cursor', 'eof', 'slot'):
        assert not hasattr(RequestBroker, name), name
    # Both port documentations are reachable and results are immutable.
    assert dataclasses.is_dataclass(Reply)
    return 2


async def verify_components():
    scenarios = [
        scenario_reader_out_of_order,
        scenario_reader_idle_repair,
        scenario_reader_later_error,
        scenario_reader_cancel,
        scenario_reader_close_failure,
        scenario_broker,
    ]
    requests = 0
    for scenario in scenarios:
        async with asyncio.timeout(5):
            requests += await scenario()
    verify_layout()
    print('COMPONENT_OK scenarios=%d requests=%d layout=deps+assembly'
          % (len(scenarios) - 1, requests))
    return requests
