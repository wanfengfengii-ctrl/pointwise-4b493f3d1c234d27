"""Shared immutable result type and port protocols.

This module deliberately contains no business logic.  It is the common vocabulary
used by the connection proxy (broker.py), the file reader (reader.py) and their
stand-alone replacements in verification.
"""
from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class Reply:
    """One immutable operation result.

    ``operation`` is the operation that produced it (OPEN, READ or CLOSE), not
    the wire packet type.  ``tag`` is an opaque, immutable correlation value
    supplied by the reader; the broker carries it but never interprets it.
    """
    operation: str
    kind: str                       # 'HANDLE', 'DATA' or 'STATUS'
    tag: object = None
    payload: Optional[bytes] = None  # HANDLE bytes or DATA bytes
    code: Optional[int] = None      # STATUS code
    message: Optional[str] = None   # STATUS message


class RoutePort(Protocol):
    """The public per-owner port published by RequestBroker.

    ``handler(reply)`` is invoked synchronously exactly once per routed reply,
    after the request has left the in-flight set and before the broker resumes
    receiving.  ``notify()`` follows every handler call (including replies the
    handler ignores) and is also called when the connection breaks.
    """

    def submit_open(self, path: str) -> None: ...

    def submit_read(self, handle: bytes, offset: int, length: int,
                    tag: object) -> None: ...

    def submit_close(self, handle: bytes) -> None: ...

    @property
    def outstanding(self) -> int:
        """Number of submitted requests for this owner that lack a reply."""

    @property
    def broken(self) -> Optional[BaseException]:
        """The connection failure, if the proxy has stopped."""

    async def wait_idle(self) -> None:
        """Return when this owner has no request awaiting a reply.

        Raises the proxy's failure if the connection breaks while waiting.
        """


class BrokerPort(Protocol):
    """The public factory used by FileReader; nothing else is required."""

    def open_route(self, handler, notify) -> RoutePort: ...


class ChannelPort(Protocol):
    """The only channel surface RequestBroker is allowed to use."""

    def send(self, packet: bytes) -> None: ...

    async def recv(self) -> bytes: ...
