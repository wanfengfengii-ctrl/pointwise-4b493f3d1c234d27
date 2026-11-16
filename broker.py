"""Exclusive connection proxy: request identity, in-flight tracking, routing.

The broker owns the channel, the monotonically increasing request ids, the
in-flight table and reply routing.  It treats READ offsets and lengths as
opaque operation arguments and has no notion of files, slots, EOF or delivery.
"""
import asyncio

from codec import command, decode
from wire import Reply


class BrokerRoute:
    """Broker-owned per-owner route; handed to exactly one consumer."""

    def __init__(self, broker, handler, notify):
        self._broker = broker
        self._handler = handler
        self._notify_owner = notify
        self._ids = set()
        self._idle = asyncio.Event()
        self._idle.set()

    # -- public consumer port -------------------------------------------------
    def submit_open(self, path):
        self._broker._submit(self, 'OPEN', None, path)

    def submit_read(self, handle, offset, length, tag):
        self._broker._submit(self, 'READ', tag, handle, offset, length)

    def submit_close(self, handle):
        self._broker._submit(self, 'CLOSE', None, handle)

    @property
    def outstanding(self):
        return len(self._ids)

    @property
    def broken(self):
        return self._broker._broken

    async def wait_idle(self):
        """Wait until this owner has no in-flight requests."""
        while self._ids:
            if self._broker._broken is not None:
                raise self._broker._broken
            self._idle.clear()
            await self._idle.wait()
        if self._broker._broken is not None:
            raise self._broker._broken

    # -- broker-side routing --------------------------------------------------
    def _add(self, ident):
        self._ids.add(ident)
        self._idle.clear()

    def _remove(self, ident):
        self._ids.discard(ident)
        if not self._ids:
            self._idle.set()

    def _deliver(self, reply):
        self._handler(reply)

    def _notify(self):
        self._notify_owner()

    def _abort(self):
        self._ids.clear()
        self._idle.set()
        self._notify_owner()


class RequestBroker:
    """Owns one channel and every request/reply that crosses it."""

    def __init__(self, channel, trace):
        self._channel = channel
        self._trace = trace
        self._serial = 0
        self._pending = {}          # ident -> (route, operation, tag)
        self._routes = []
        self._pump = None
        self._broken = None

    def open_route(self, handler, notify):
        route = BrokerRoute(self, handler, notify)
        self._routes.append(route)
        return route

    def _submit(self, route, operation, tag, *args):
        if self._broken is not None:
            raise self._broken
        if self._serial == 0xffffffff:
            raise OverflowError('request ids exhausted')
        self._serial += 1
        ident = self._serial
        self._pending[ident] = (route, operation, tag)
        route._add(ident)
        if operation == 'OPEN':
            packet = command('OPEN', ident, args[0])
        elif operation == 'CLOSE':
            packet = command('CLOSE', ident, args[0])
        else:
            handle, offset, length = args
            packet = command('READ', ident, handle, offset, length)
        self._channel.send(packet)
        if self._pump is None:
            self._pump = asyncio.create_task(self._receive())

    async def _receive(self):
        try:
            while True:
                decoded = decode(await self._channel.recv())
                self._trace.append(['recv'] + decoded)
                typ, ident = decoded[:2]
                route, operation, tag = self._pending.pop(ident)
                route._remove(ident)
                route._deliver(self._result(operation, tag, decoded))
                route._notify()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._broken = exc
            self._pending.clear()
            for route in self._routes:
                route._abort()

    @staticmethod
    def _result(operation, tag, decoded):
        typ = decoded[0]
        if typ == 'HANDLE':
            return Reply(operation, 'HANDLE', tag, bytes.fromhex(decoded[2]))
        if typ == 'DATA':
            return Reply(operation, 'DATA', tag, bytes.fromhex(decoded[2]))
        return Reply(operation, 'STATUS', tag, None, decoded[2], decoded[3])

    async def aclose(self):
        if self._pump is not None:
            self._pump.cancel()
            try:
                await self._pump
            except asyncio.CancelledError:
                pass
