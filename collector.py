"""Assembly-only Collector: validates arguments and wires a channel to files."""
from broker import RequestBroker
from reader import FileReader, SFTPError


class Collector:
    def __init__(self, channel, trace):
        self.trace = trace
        self.broker = RequestBroker(channel, trace)
        self.files = []

    def iter_file(self, path, block=4, window=2):
        if not isinstance(path, str) or not path:
            raise ValueError('path')
        if type(block) is not int or type(window) is not int or block <= 0 or window <= 0:
            raise ValueError('limits')
        f = FileReader(self.broker, path, block, window, self.trace)
        self.files.append(f)
        return f

    async def aclose(self):
        if any(not f.closed for f in self.files):
            raise ValueError('files still open')
        await self.broker.aclose()
