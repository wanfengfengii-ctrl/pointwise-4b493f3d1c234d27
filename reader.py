"""Exclusive file reader: handle lifetime, offsets, assembly, delivery, cleanup.

A FileReader owns one file's mutable state.  It talks to the connection only
through the public route port it was given (``submit_open`` / ``submit_read`` /
``submit_close`` / ``wait_idle`` / ``broken``), and it never sees request ids,
the wire channel or another file's state.  Replies reach it as immutable
``wire.Reply`` values; the opaque tag on a reply is the immutable block offset
this reader chose, which it maps to its own slot table.
"""
import asyncio

from wire import Reply


class SFTPError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


class FileReader:
    """Async bytes iterator over one stable file, block by block."""

    def __init__(self, broker, path, block, window, trace):
        self._broker = broker
        self.path = path
        self.block = block
        self.window = window
        self._trace = trace

        self._handle = None
        self._opened = False
        self._open_error = None
        self._slots = {}          # start offset -> slot dict, delivery order
        self._cursor = 0
        self._next_offset = 0
        self._eof = None
        self._stopping = False
        self._closed = False
        self._busy = False
        self._close_error = None
        self._changed = asyncio.Event()
        # The only connection surface this reader may use.
        self._route = broker.open_route(self._on_reply, self._changed.set)

    # -- read-only view used by the assembler --------------------------------
    @property
    def closed(self):
        return self._closed

    # -- iterator protocol ----------------------------------------------------
    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._busy:
            raise ValueError('concurrent file operation')
        self._busy = True
        try:
            return await self._next()
        finally:
            self._busy = False

    async def aclose(self):
        if self._busy:
            raise ValueError('concurrent file operation')
        self._busy = True
        try:
            await self._finish()
        finally:
            self._busy = False

    # -- sending through the public route port --------------------------------
    def _request_read(self, slot):
        self._route.submit_read(self._handle,
                                slot['start'] + len(slot['data']),
                                self.block - len(slot['data']),
                                slot['start'])

    def _fill(self):
        while len(self._slots) < self.window and self._eof is None:
            slot = {'start': self._next_offset, 'data': bytearray(),
                    'done': False, 'error': None}
            self._slots[self._next_offset] = slot
            self._next_offset += self.block
            self._request_read(slot)

    # -- reply handling (called synchronously by the route) -------------------
    def _on_reply(self, reply):
        if not isinstance(reply, Reply):
            raise TypeError('reply')
        operation = reply.operation
        if operation == 'OPEN':
            self._opened = True
            if reply.kind == 'HANDLE':
                self._handle = reply.payload
            else:
                self._open_error = SFTPError(reply.code, reply.message)
        elif operation == 'CLOSE':
            if reply.code != 0:
                self._close_error = SFTPError(reply.code, reply.message)
        elif not self._stopping and not self._closed:
            slot = self._slots.get(reply.tag)
            if slot is not None:
                self._absorb(slot, reply)
        self._changed.set()

    def _absorb(self, slot, reply):
        if reply.kind == 'DATA':
            data = reply.payload or b''
            if not data or len(data) > self.block - len(slot['data']):
                raise ValueError('DATA length')
            slot['data'].extend(data)
            if len(slot['data']) < self.block:
                # Repair right here, before the route consumes another reply.
                self._request_read(slot)
            else:
                slot['done'] = True
        elif reply.code == 1:
            slot['done'] = True
            end = slot['start'] + len(slot['data'])
            self._eof = end if self._eof is None else min(end, self._eof)
        else:
            slot['error'] = SFTPError(reply.code, reply.message)
            slot['done'] = True

    # -- waiting --------------------------------------------------------------
    async def _wait(self, predicate):
        while not predicate():
            if self._route.broken is not None:
                raise self._route.broken
            self._changed.clear()
            await self._changed.wait()
        if self._route.broken is not None:
            raise self._route.broken

    # -- delivery -------------------------------------------------------------
    async def _next(self):
        self._trace.append(['pull', self.path])
        if self._closed:
            raise StopAsyncIteration
        try:
            if not self._opened:
                self._route.submit_open(self.path)
                await self._wait(lambda: self._opened)
                if self._open_error:
                    raise self._open_error
            self._fill()
            if self._eof is not None and self._cursor >= self._eof:
                await self._finish()
                raise StopAsyncIteration
            slot = self._slots[self._cursor]
            await self._wait(lambda: slot['done'])
            if slot['error']:
                raise slot['error']
            data = bytes(slot['data'])
            if not data:
                await self._finish()
                raise StopAsyncIteration
            del self._slots[self._cursor]
            self._cursor += self.block
            self._trace.append(['yield', self.path, data.hex()])
            return data
        except StopAsyncIteration:
            raise
        except BaseException as exc:
            await self._finish(exc)

    async def _finish(self, primary=None):
        if self._closed:
            if primary is not None:
                raise primary
            return
        self._stopping = True
        self._trace.append(['stop', self.path,
                            type(primary).__name__ if primary else ''])
        cleanup = None
        try:
            # Only requests that actually crossed the connection may block this.
            await self._route.wait_idle()
            if self._handle is not None:
                self._route.submit_close(self._handle)
                await self._route.wait_idle()
                cleanup = self._close_error
        except BaseException as exc:
            cleanup = exc
        finally:
            self._closed = True
            self._slots.clear()
            self._trace.append(['closed', self.path])
        if primary is not None:
            raise primary
        if cleanup is not None:
            raise cleanup
