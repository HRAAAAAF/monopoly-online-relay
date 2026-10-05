"""Networking layer for the Monopoly online mod: wire protocol and session server.

Imports are deliberately lazy. Eagerly importing `.server` here would put
`net.server` in `sys.modules` before `python -m net.server` executes it, which
emits a RuntimeWarning, and it would also drag asyncio into anything that only
wanted the pure framing helpers.
"""

from .protocol import (
    PROTOCOL_VERSION,
    ProtocolError,
    FrameReader,
    decode_frame,
    encode,
    validate,
)

__all__ = [
    "PROTOCOL_VERSION",
    "ProtocolError",
    "FrameReader",
    "decode_frame",
    "encode",
    "validate",
    "SessionServer",
    "Room",
    "generate_code",
    "roll_die",
]

_LAZY = {"SessionServer", "Room", "generate_code", "roll_die"}


def __getattr__(name: str):
    if name in _LAZY:
        from . import server
        return getattr(server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals()))
