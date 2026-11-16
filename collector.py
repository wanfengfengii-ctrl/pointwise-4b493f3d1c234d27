"""Working coupled baseline: all routing, file semantics and cleanup live here."""
import asyncio
from codec import command, decode

class SFTPError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)

class _File:
    def __init__(self, owner, path, block, window):
        self.owner, self.path = owner, path
        self.block, self.window = block, window
        self.handle = None
        self.opened = False
        self.open_error = None
        self.ids = set()
        self.slots = {}
        self.cursor = self.next_offset = 0
        self.eof = None
        self.stopping = self.closed = self.busy = False
        self.close_error = None
        self.changed = asyncio.Event()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.busy:
            raise ValueError('concurrent file operation')
        self.busy = True
        try:
            return await self.owner._next(self)
        finally:
            self.busy = False

    async def aclose(self):
        if self.busy:
            raise ValueError('concurrent file operation')
        self.busy = True
        try:
            await self.owner._finish(self)
        finally:
            self.busy = False

class Collector:
    def __init__(self, channel, trace):
        self.channel, self.trace = channel, trace
        self.pending = {}
        self.serial = 0
        self.pump = None
        self.files = []
        self.broken = None

    def iter_file(self, path, block=4, window=2):
        if not isinstance(path, str) or not path:
            raise ValueError('path')
        if type(block) is not int or type(window) is not int or block <= 0 or window <= 0:
            raise ValueError('limits')
        f = _File(self, path, block, window)
        self.files.append(f)
        return f

    def _send(self, f, kind, slot=None):
        if self.broken:
            raise self.broken
        if self.serial == 0xffffffff:
            raise OverflowError('request ids exhausted')
        self.serial += 1
        ident = self.serial
        # A request record directly shares its file and mutable assembly slot.
        r = {'file': f, 'kind': kind, 'slot': slot}
        self.pending[ident] = r
        f.ids.add(ident)
        if kind == 'OPEN':
            args = (f.path,)
        elif kind == 'CLOSE':
            args = (f.handle,)
        else:
            args = (f.handle, slot['start'] + len(slot['data']),
                    f.block - len(slot['data']))
        self.channel.send(command(kind, ident, *args))
        if self.pump is None:
            self.pump = asyncio.create_task(self._receive())

    async def _receive(self):
        try:
            while True:
                reply = decode(await self.channel.recv())
                self.trace.append(['recv'] + reply)
                typ, ident = reply[:2]
                r = self.pending.pop(ident)
                f, kind, slot = r['file'], r['kind'], r['slot']
                f.ids.remove(ident)
                if kind == 'OPEN':
                    f.opened = True
                    if typ == 'HANDLE':
                        f.handle = bytes.fromhex(reply[2])
                    else:
                        f.open_error = SFTPError(reply[2], reply[3])
                elif kind == 'CLOSE':
                    if reply[2] != 0:
                        f.close_error = SFTPError(reply[2], reply[3])
                elif not f.stopping:
                    if typ == 'DATA':
                        data = bytes.fromhex(reply[2])
                        if not data or len(data) > f.block - len(slot['data']):
                            raise ValueError('DATA length')
                        slot['data'].extend(data)
                        if len(slot['data']) < f.block:
                            # Eager repair in the connection pump, even without a pull.
                            self._send(f, 'READ', slot)
                        else:
                            slot['done'] = True
                    elif reply[2] == 1:
                        slot['done'] = True
                        end = slot['start'] + len(slot['data'])
                        f.eof = end if f.eof is None else min(end, f.eof)
                    else:
                        slot['error'] = SFTPError(reply[2], reply[3])
                        slot['done'] = True
                f.changed.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.broken = exc
            self.pending.clear()
            for f in self.files:
                f.ids.clear()
                f.changed.set()

    async def _wait(self, f, predicate):
        while not predicate():
            if self.broken:
                raise self.broken
            f.changed.clear()
            await f.changed.wait()
        if self.broken:
            raise self.broken

    def _fill(self, f):
        while len(f.slots) < f.window and f.eof is None:
            slot = {'start': f.next_offset, 'data': bytearray(),
                    'done': False, 'error': None}
            f.slots[f.next_offset] = slot
            f.next_offset += f.block
            self._send(f, 'READ', slot)

    async def _next(self, f):
        self.trace.append(['pull', f.path])
        if f.closed:
            raise StopAsyncIteration
        try:
            if not f.opened:
                self._send(f, 'OPEN')
                await self._wait(f, lambda: f.opened)
                if f.open_error:
                    raise f.open_error
            self._fill(f)
            if f.eof is not None and f.cursor >= f.eof:
                await self._finish(f)
                raise StopAsyncIteration
            slot = f.slots[f.cursor]
            await self._wait(f, lambda: slot['done'])
            if slot['error']:
                raise slot['error']
            data = bytes(slot['data'])
            if not data:
                await self._finish(f)
                raise StopAsyncIteration
            del f.slots[f.cursor]
            f.cursor += f.block
            self.trace.append(['yield', f.path, data.hex()])
            return data
        except StopAsyncIteration:
            raise
        except BaseException as exc:
            await self._finish(f, exc)

    async def _finish(self, f, primary=None):
        if f.closed:
            if primary is not None:
                raise primary
            return
        f.stopping = True
        self.trace.append(['stop', f.path, type(primary).__name__ if primary else ''])
        cleanup = None
        try:
            await self._wait(f, lambda: not f.ids)
            if f.handle is not None:
                self._send(f, 'CLOSE')
                await self._wait(f, lambda: not f.ids)
                cleanup = f.close_error
        except BaseException as exc:
            cleanup = exc
        finally:
            f.closed = True
            f.slots.clear()
            self.trace.append(['closed', f.path])
        if primary is not None:
            raise primary
        if cleanup is not None:
            raise cleanup

    async def aclose(self):
        if any(not f.closed for f in self.files):
            raise ValueError('files still open')
        if self.pump is not None:
            self.pump.cancel()
            try:
                await self.pump
            except asyncio.CancelledError:
                pass
