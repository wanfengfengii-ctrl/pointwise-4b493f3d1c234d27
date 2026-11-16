"""A controllable, framed SFTP channel, with no network or server dependency."""
import asyncio
from codec import decode, response

class Channel:
    def __init__(self, trace):
        self.trace = trace
        self.commands = []
        self.incoming = asyncio.Queue()
        self.replied = set()

    def send(self, packet):
        item = decode(packet)
        self.commands.append(item)
        self.trace.append(['send'] + item)

    async def recv(self):
        return await self.incoming.get()

    def reply(self, command_index, kind, value):
        item = self.commands[command_index]
        ident = item[1]
        if ident in self.replied:
            raise ValueError('duplicate scripted reply')
        self.replied.add(ident)
        self.incoming.put_nowait(response(ident, kind, value))
