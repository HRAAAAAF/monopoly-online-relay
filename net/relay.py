"""Internet room relay. TLS is terminated by the deployment's HTTPS proxy.

Run one instance with a recovery file; disconnects pause native replay.
No original game binaries, assets or process access are needed on this server.
"""
import asyncio
import http
import os
import signal
import time

from .server import SessionServer
from .protocol import MAX_FRAME
from .ws_transport import websocket_library, WebSocketReader, WebSocketWriter


class RoomRelay:
    def __init__(self, max_connections=64, recovery_ttl=1800, recovery_path=None):
        self.sessions = SessionServer(recovery_path=recovery_path,recovery_ttl=recovery_ttl)
        self.server = None
        self.active = 0
        self.max_connections = max_connections
        self.recovery_ttl = recovery_ttl

    def reclaim(self):
        # Retain a started native room when every connection drops together.
        # Its tokens and journal are needed to verify both returning seats.
        now=time.monotonic()
        for code,room in list(self.sessions.rooms.items()):
            if any(p.connected for p in room.players.values()):continue
            if room.native_mode and room.started and now-room.last_activity<self.recovery_ttl:continue
            self.sessions.rooms.pop(code,None)
            for player in room.players.values():player.room=None
        for pid,player in list(self.sessions.players.items()):
            if not player.connected and player.room is None:
                self.sessions.players.pop(pid,None)
                self.sessions.tokens.pop(player.token,None)
        self.sessions.persist()

    def request(self, connection, request):
        if request.path == "/healthz":
            return connection.respond(http.HTTPStatus.OK, "OK\n")
        if request.path != "/session":
            return connection.respond(http.HTTPStatus.NOT_FOUND, "Not found\n")
        # Desktop clients don't send browser Origin headers. This also rejects
        # unrelated webpages trying to open a session through a browser.
        if request.headers.get_all("Origin"):
            return connection.respond(http.HTTPStatus.FORBIDDEN, "Desktop clients only\n")
        if self.active >= self.max_connections:
            return connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "Relay full\n")

    async def handle(self, websocket):
        self.reclaim()
        if self.active >= self.max_connections:
            await websocket.close(code=1013, reason="Relay full")
            return
        self.active += 1
        try:
            await self.sessions.handle(WebSocketReader(websocket), WebSocketWriter(websocket),
                                       hello_timeout=10, message_rate=60)
        finally:
            self.active -= 1
            self.reclaim()

    async def start(self, host="127.0.0.1", port=0, *, ssl_context=None):
        websocket_library()
        from websockets.asyncio.server import serve
        self.server = await serve(self.handle, host, port, process_request=self.request,
                                  max_size=MAX_FRAME + 4, max_queue=8, compression=None,
                                  ping_interval=20, ping_timeout=20, close_timeout=3,
                                  server_header=None, ssl=ssl_context)
        return self.server

    def bound_port(self):
        return self.server.sockets[0].getsockname()[1]

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None


async def run():
    relay = RoomRelay(recovery_path=os.environ.get('MONOPOLY_RECOVERY_PATH','.relay-state/rooms.json'))
    server = await relay.start("0.0.0.0", int(os.environ.get("PORT", "10000")))
    loop = asyncio.get_running_loop()
    if os.name != "nt":
        loop.add_signal_handler(signal.SIGTERM, server.close)
    try:
        await server.wait_closed()
    finally:
        await relay.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
