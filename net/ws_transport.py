"""Bounded binary WebSocket transport for the existing framed game protocol."""
import asyncio
import ipaddress
import sys
import ssl
from collections import deque
from pathlib import Path
from urllib.parse import urlsplit

from .protocol import MAX_FRAME, ProtocolError


def websocket_library():
    try:
        import websockets
    except ImportError:
        # Optional project-local runtime installed for the Windows preview.
        runtime = Path(__file__).resolve().parents[1] / ".runtime"
        if runtime.is_dir():
            sys.path.insert(0, str(runtime))
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("Internet mode requires requirements-relay.txt") from exc
    return websockets


def validate_relay_url(url):
    try:
        parsed = urlsplit(url)
        host, port = parsed.hostname, parsed.port
    except (TypeError, ValueError) as exc:
        raise ProtocolError("Invalid relay address") from exc
    if (not host or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment or parsed.path not in ("", "/session")):
        raise ProtocolError("Use a relay address such as wss://server.example/session")
    if port is not None and not 1 <= port <= 65535:
        raise ProtocolError("Invalid relay port")
    local = host.lower() == "localhost"
    try:
        local = local or ipaddress.ip_address(host).is_loopback
    except ValueError:
        pass
    if parsed.scheme != "wss" and not (parsed.scheme == "ws" and local):
        raise ProtocolError("Internet connections require encrypted wss:// transport")
    return url


class WebSocketReader:
    def __init__(self, websocket):
        self.websocket = websocket
        self.buffer = bytearray()

    async def readexactly(self, size):
        if not 0 <= size <= MAX_FRAME + 4:
            raise ProtocolError("Invalid WebSocket read length")
        while len(self.buffer) < size:
            try:
                message = await self.websocket.recv()
            except websocket_library().exceptions.ConnectionClosed as exc:
                partial = bytes(self.buffer)
                self.buffer.clear()
                raise asyncio.IncompleteReadError(partial, size) from exc
            if not isinstance(message, bytes) or not message:
                raise ConnectionError("Relay requires nonempty binary protocol frames")
            if len(self.buffer) + len(message) > MAX_FRAME + 4:
                raise ConnectionError("Relay receive buffer limit exceeded")
            self.buffer.extend(message)
        data = bytes(self.buffer[:size])
        del self.buffer[:size]
        return data


class WebSocketWriter:
    def __init__(self, websocket):
        self.websocket = websocket
        self.pending = deque()
        self.pending_bytes = 0
        self.lock = asyncio.Lock()
        self.closing = False
        self.close_task = None

    def get_extra_info(self, name):
        return self.websocket.remote_address if name == "peername" else None

    def is_closing(self):
        return self.closing or self.websocket.state.name in ("CLOSING", "CLOSED")

    def write(self, data):
        if self.is_closing():
            raise ConnectionError("Relay connection closed")
        if not isinstance(data, bytes) or not 0 < len(data) <= MAX_FRAME + 4:
            raise ProtocolError("Invalid outgoing relay frame")
        if self.pending_bytes + len(data) > 2 * (MAX_FRAME + 4):
            raise ConnectionError("Relay send buffer limit exceeded")
        self.pending.append(data)
        self.pending_bytes += len(data)

    async def drain(self):
        async with self.lock:
            while self.pending:
                frame = self.pending.popleft()
                self.pending_bytes -= len(frame)
                try:
                    await asyncio.wait_for(self.websocket.send(frame), 10)
                except (websocket_library().exceptions.ConnectionClosed, asyncio.TimeoutError) as exc:
                    self.close()
                    raise ConnectionError("Relay send failed") from exc

    def close(self):
        if not self.closing:
            self.closing = True
            self.pending.clear()
            self.pending_bytes = 0
            self.close_task = asyncio.create_task(self.websocket.close())

    async def wait_closed(self):
        if self.close_task:
            await self.close_task
        await self.websocket.wait_closed()


async def open_relay(url, timeout=20):
    validate_relay_url(url)
    websocket_library()
    from websockets.asyncio.client import connect
    options = {"ssl": ssl.create_default_context()} if urlsplit(url).scheme == "wss" else {}
    websocket = await connect(url, open_timeout=timeout, max_size=MAX_FRAME + 4,
                              max_queue=8, compression=None, ping_interval=20,
                              ping_timeout=20, close_timeout=3, proxy=None, **options)
    return WebSocketReader(websocket), WebSocketWriter(websocket)
