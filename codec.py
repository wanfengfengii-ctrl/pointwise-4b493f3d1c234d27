"""Finite SFTP v3 packet codec; the channel supplies whole framed packets."""
import struct

def u32(n):
    return struct.pack('>I', n)

def string(b):
    return u32(len(b)) + b

def frame(kind, ident, body=b''):
    b = bytes([kind]) + u32(ident) + body
    return u32(len(b)) + b

def command(kind, ident, *args):
    if kind == 'OPEN':
        return frame(3, ident, string(args[0].encode()) + u32(1) + u32(0))
    if kind == 'READ':
        h, off, size = args
        return frame(5, ident, string(h) + struct.pack('>Q', off) + u32(size))
    if kind == 'CLOSE':
        return frame(4, ident, string(args[0]))
    raise ValueError(kind)

def response(ident, kind, value):
    if kind == 'HANDLE':
        return frame(102, ident, string(value))
    if kind == 'DATA':
        return frame(103, ident, string(value))
    if kind == 'STATUS':
        code, message = value
        return frame(101, ident, u32(code) + string(message.encode()) + string(b''))
    raise ValueError(kind)

def decode(packet):
    if len(packet) < 9 or struct.unpack('>I', packet[:4])[0] != len(packet) - 4:
        raise ValueError('frame')
    kind, ident = packet[4], struct.unpack('>I', packet[5:9])[0]
    pos = 9
    def number(width=4):
        nonlocal pos
        end = pos + width
        if end > len(packet):
            raise ValueError('truncated number')
        n = int.from_bytes(packet[pos:end], 'big')
        pos = end
        return n
    def blob():
        nonlocal pos
        n = number()
        end = pos + n
        if end > len(packet):
            raise ValueError('truncated string')
        b = packet[pos:end]
        pos = end
        return b
    if kind == 3:
        path = blob().decode()
        if number() != 1 or number() != 0:
            raise ValueError('OPEN flags')
        result = ['OPEN', ident, path]
    elif kind == 4:
        result = ['CLOSE', ident, blob().hex()]
    elif kind == 5:
        result = ['READ', ident, blob().hex(), number(8), number()]
    elif kind in (102, 103):
        result = ['HANDLE' if kind == 102 else 'DATA', ident, blob().hex()]
    elif kind == 101:
        code, message, language = number(), blob().decode(), blob()
        result = ['STATUS', ident, code, message]
    else:
        raise ValueError('packet type')
    if pos != len(packet):
        raise ValueError('trailing bytes')
    return result
