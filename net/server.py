"""Authoritative session server for the Monopoly online mod.

What lives here and why
-----------------------
1. **Invite-code rooms.** Friends join with a 6-character code instead of an IP.
2. **Server-side dice.** Rolled with ``secrets``, so a modified client cannot
   pick its own numbers. This is the whole point of having a server at all for
   a turn-based board game.
3. **Turn ownership.** Only the seated current player's actions are accepted;
   everyone else gets an error and the room's state is unchanged.
4. **Reconnection.** A player token keeps a seat across a dropped connection,
   which matters a lot when the client is an injected DLL inside a game that
   occasionally stutters or reloads.

What deliberately is NOT here: board rules (rent, auctions, building). Those
live in the game; the mod mirrors the authoritative events this server emits.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import secrets
import struct
import time
import copy
from collections import deque
from typing import Optional

try:  # running as a package (tests, `python -m net.server`)
    from .protocol import (
        CODE_ALPHABET, CODE_LENGTH, PROTOCOL_VERSION,
        C_ACTION, C_CHAT, C_CREATE, C_HELLO, C_JOIN, C_LEAVE, C_PING, C_START, C_READY, C_ACK, C_FAULT, C_RESUME, C_REPLAY_ACK,
        S_BYE, S_CHAT, S_ERROR, S_EVENT, S_PONG, S_ROOM, S_STATE, S_WELCOME,
        ProtocolError, decode_frame, encode, validate,
    )
except ImportError:  # pragma: no cover - direct script execution
    from protocol import (  # type: ignore
        CODE_ALPHABET, CODE_LENGTH, PROTOCOL_VERSION,
        C_ACTION, C_CHAT, C_CREATE, C_HELLO, C_JOIN, C_LEAVE, C_PING, C_START, C_READY, C_ACK, C_FAULT, C_RESUME, C_REPLAY_ACK,
        S_BYE, S_CHAT, S_ERROR, S_EVENT, S_PONG, S_ROOM, S_STATE, S_WELCOME,
        ProtocolError, decode_frame, encode, validate,
    )

log = logging.getLogger("monopoly.server")

MAX_PLAYERS = 4  # Verified native Game has four inline Player slots.
DICE_SIDES = 6
MAX_DOUBLES = 3
NATIVE_FEATURES = ['resume-v1','native-match-v2']


def generate_code() -> str:
    return "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LENGTH))


def roll_die() -> int:
    """One fair die, from the OS CSPRNG."""
    return secrets.randbelow(DICE_SIDES) + 1


class Player:
    __slots__ = ("pid", "name", "token", "writer", "room", "connected", "recovering", "recovery_seq")

    def __init__(self, pid: int, name: str, token: str) -> None:
        self.pid = pid
        self.name = name
        self.token = token
        self.writer: Optional[asyncio.StreamWriter] = None
        self.room: Optional["Room"] = None
        self.connected = False
        self.recovering = False
        self.recovery_seq = None

    def public(self) -> dict:
        return {
            "id": self.pid,
            "name": self.name,
            "connected": self.connected,
            "host": bool(self.room and self.room.host_pid == self.pid),
        }


class Room:
    def __init__(self, code: str, host: Player) -> None:
        self.code = code
        self.host_pid = host.pid
        self.players: dict[int, Player] = {}
        self.started = False
        self.finished = False
        self.winner_pid = None
        self.active_seats = None
        self.preparing = False
        self.last_activity = time.monotonic()
        self.turn_pid: Optional[int] = None
        self.doubles_count = 0
        self.rolls: list[list[int]] = []
        self.seq = 0
        self.event_seq = 0
        self.action_lock = asyncio.Lock()
        self.broadcast_lock = asyncio.Lock()
        self.native_mode = False
        self.native_setup = {"decks": [list(range(16)), list(range(16))], "cursors": [15, 15]}
        for deck in self.native_setup["decks"]:
            secrets.SystemRandom().shuffle(deck)
        self.ready_checkpoints = {}
        self.pending_event = None
        self.pending_kind = None
        self.native_acks = {}
        self.native_fault = None
        self.event_history = {}
        self.checkpoints = {}
        self.native_decision = None
        self.decision_reports = {}
        self.decisions = {0: None}
        self.add(host)

    # -- membership ---------------------------------------------------------
    def record_decision(self, pid, decision):
        if decision is not None and decision['actor_index'] >= len(self.players):
            self.native_fault = 'Native decision refers to an absent seat'
            return
        if decision and 'active' in decision and any(index>=len(self.players) for index in decision['active']):
            self.native_fault='Native surviving-seat report refers to an absent seat'
            return
        if pid in self.decision_reports and self.decision_reports[pid] != decision:
            self.native_fault = 'Client changed its native decision'
            return
        self.decision_reports[pid] = copy.deepcopy(decision)
        if any(value != decision for value in self.decision_reports.values()):
            self.native_fault = 'Native decisions differ between game copies'

    def complete_native_event(self, digest):
        self.checkpoints[self.pending_event] = digest
        self.native_decision = copy.deepcopy(next(iter(self.decision_reports.values()), None))
        self.decisions[self.pending_event] = copy.deepcopy(self.native_decision)
        if self.native_decision and self.native_decision['kind']=='winner':
            self.finished=True
            self.winner_pid=list(self.players)[self.native_decision['actor_index']]
            self.turn_pid=self.winner_pid
        if self.native_decision and self.native_decision['kind'] == 'turn':
            if 'active' in self.native_decision:
                self.active_seats=[list(self.players)[index] for index in self.native_decision['active']]
            observed_turn=list(self.players)[self.native_decision['actor_index']]
            if observed_turn != self.turn_pid:
                self.doubles_count=0
            self.turn_pid=observed_turn
        self.pending_event = None

    def add(self, player: Player) -> None:
        if len(self.players) >= MAX_PLAYERS:
            raise ProtocolError(f"room is full ({MAX_PLAYERS} players)")
        self.players[player.pid] = player
        player.room = self
        self.ready_checkpoints.clear()

    def remove(self, player: Player) -> None:
        if self.preparing:
            self.preparing = False
            self.native_fault = "A player left during game setup; create a new room"
        self.players.pop(player.pid, None)
        if player.room is self:
            player.room = None
        self.ready_checkpoints.clear()
        if self.native_mode and self.started:
            self.native_fault = "Native seat left; start a new room"
        if not self.players:
            return
        if self.host_pid == player.pid:
            # Promote the longest-seated remaining player.
            self.host_pid = next(iter(self.players))
            log.info("room %s: host promoted to pid %s", self.code, self.host_pid)
        if self.turn_pid == player.pid and not (self.native_mode and self.started):
            self.advance_turn()

    def has(self, pid: int) -> bool:
        return pid in self.players

    def current(self) -> Optional[Player]:
        return self.players.get(self.turn_pid) if self.turn_pid else None

    # -- turn handling ------------------------------------------------------
    def advance_turn(self) -> None:
        order = [pid for pid in self.players if self.active_seats is None or pid in self.active_seats]
        if not order:
            self.turn_pid = None
            return
        if self.turn_pid in order:
            idx = (order.index(self.turn_pid) + 1) % len(order)
        else:
            idx = 0
        self.turn_pid = order[idx]
        self.doubles_count = 0

    def start(self) -> None:
        self.preparing = False
        self.started = True
        self.rolls = []
        self.doubles_count = 0
        self.turn_pid = next(iter(self.players))

    # -- snapshots ----------------------------------------------------------
    def room_message(self) -> dict:
        return {
            "t": S_ROOM,
            "code": self.code,
            "host": self.host_pid,
            "started": self.started,
            "preparing": self.preparing,
            "native": self.native_mode,
            "native_setup": self.native_setup if self.native_mode else None,
            "turn": self.turn_pid,
            "players": [p.public() for p in self.players.values()],
        }

    def state_message(self) -> dict:
        return {
            "t": S_STATE,
            "code": self.code,
            "seq": self.seq,
            "event_seq": self.event_seq,
            "native": self.native_mode,
            "awaiting_native_event": self.pending_event,
            "native_ready": sorted(self.ready_checkpoints),
            "native_acks": sorted(self.native_acks),
            "native_fault": self.native_fault,
            "recovering": [p.pid for p in self.players.values() if p.recovering],
            "decision": self.native_decision,
            "finished":self.finished,
            "winner":self.winner_pid,
            "turn": self.turn_pid,
            "doubles": self.doubles_count,
            "rolls": self.rolls[-16:],
        }


class SessionServer:
    """The authoritative session layer."""

    def __init__(self, *, recovery_path=None, recovery_ttl=1800) -> None:
        self.rooms: dict[str, Room] = {}
        self.players: dict[int, Player] = {}
        self.tokens: dict[str, Player] = {}
        self._next_pid = 1
        self._server: Optional[asyncio.AbstractServer] = None
        self.recovery_path = recovery_path
        if recovery_path is not None:
            from .recovery_store import restore
            restore(self,recovery_path,recovery_ttl)

    def persist(self):
        if self.recovery_path is not None:
            from .recovery_store import save
            save(self,self.recovery_path)

    # -- helpers ------------------------------------------------------------
    def _new_player(self, name: str, token: Optional[str] = None) -> Player:
        pid = self._next_pid
        self._next_pid += 1
        token = token or secrets.token_urlsafe(18)
        player = Player(pid, name, token)
        self.players[pid] = player
        self.tokens[token] = player
        return player

    @staticmethod
    async def _send(writer: Optional[asyncio.StreamWriter], message: dict) -> bool:
        if writer is None or writer.is_closing():
            return False
        try:
            writer.write(encode(message))
            await writer.drain()
            return True
        except (ConnectionError, RuntimeError):
            return False

    async def _send_error(self, player: Optional[Player],
                          writer: Optional[asyncio.StreamWriter],
                          code: str, message: str) -> None:
        await self._send(writer or (player.writer if player else None),
                         {"t": S_ERROR, "code": code, "message": message})

    async def _broadcast(self, room: Room, message: dict) -> None:
        room.last_activity = time.monotonic()
        async with room.broadcast_lock:
            if message.get("t") == S_EVENT:
                if room.native_mode and len(room.event_history) >= 10000:
                    raise ProtocolError("Room event history limit reached; save and restart the room")
                room.event_seq += 1
                message = {**message, "code": room.code, "event_seq": room.event_seq}
                if room.native_mode:
                    room.event_history[room.event_seq] = copy.deepcopy(message)
                    if message.get("kind") == "started":
                        room.checkpoints[0] = next(iter(room.ready_checkpoints.values()))
                    room.pending_event = room.event_seq
                    room.pending_kind = message.get("kind")
                    room.native_acks.clear()
                    room.decision_reports.clear()
            # Persist the authoritative event before any client may execute it.
            self.persist()
            for player in list(room.players.values()):
                if player.connected:
                    if not await self._send(player.writer, message):
                        player.connected = False

    async def _push_room(self, room: Room) -> None:
        await self._broadcast(room, room.room_message())

    async def _push_state(self, room: Room) -> None:
        room.seq += 1
        await self._broadcast(room, room.state_message())

    # -- connection handling ------------------------------------------------
    async def handle(self, reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter, *, hello_timeout=None, message_rate=None) -> None:
        peer = writer.get_extra_info("peername")
        player: Optional[Player] = None
        arrivals = deque()
        log.info("connection from %s", peer)
        try:
            while True:
                try:
                    header = await asyncio.wait_for(reader.readexactly(4), hello_timeout if player is None else None)
                except asyncio.IncompleteReadError:
                    break
                (length,) = struct.unpack(">I", header)
                if length > (1 << 20):
                    await self._send_error(player, writer, "frame_too_large",
                                           "declared frame length is too large")
                    break
                try:
                    body = await asyncio.wait_for(reader.readexactly(length), hello_timeout if player is None else None)
                except asyncio.IncompleteReadError:
                    break

                try:
                    if message_rate is not None:
                        now = time.monotonic()
                        while arrivals and arrivals[0] < now - 1:
                            arrivals.popleft()
                        arrivals.append(now)
                        if len(arrivals) > message_rate:
                            raise ConnectionError("Session message rate exceeded")
                    message = decode_frame(body)
                    validate(message, expect="client")
                except ProtocolError as exc:
                    await self._send_error(player, writer, "bad_message", str(exc))
                    if player is None:
                        break
                    continue

                player = await self._dispatch(player, writer, message)
                if player is None:
                    break
        except (ConnectionError, asyncio.TimeoutError, asyncio.CancelledError):
            pass
        finally:
            await self._disconnect(player, writer=writer)
            with contextlib.suppress(Exception):
                writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _dispatch(self, player: Optional[Player],
                        writer: asyncio.StreamWriter,
                        message: dict) -> Optional[Player]:
        """Route one validated message. Returns the bound player (or None to close)."""
        mtype = message["t"]

        if mtype == C_HELLO:
            if player is not None:
                await self._send_error(player, writer, "already_helloed",
                                       "this connection already has a player identity")
                return player
            token = message.get("token")
            if token and token in self.tokens:
                existing = self.tokens[token]
                if existing.connected:
                    await self._send_error(None, writer, "already_connected",
                                           "that token is already in use")
                    return None
                existing.writer = writer
                existing.connected = True
                existing.name = message["name"].strip()
                if existing.room and existing.room.native_mode and existing.room.started:
                    existing.recovering = True
                    existing.recovery_seq = None
                await self._send(writer, {
                    "t": S_WELCOME, "player_id": existing.pid,
                    "token": existing.token, "protocol": PROTOCOL_VERSION,
                    "reconnected": True,
                    "native_features": NATIVE_FEATURES,
                })
                if existing.room:
                    existing.room.last_activity = time.monotonic()
                    await self._push_room(existing.room)
                    await self._push_state(existing.room)
                return existing
            player = self._new_player(message["name"].strip())
            player.writer = writer
            player.connected = True
            await self._send(writer, {
                "t": S_WELCOME, "player_id": player.pid,
                "token": player.token, "protocol": PROTOCOL_VERSION,
                "reconnected": False,
                "native_features": NATIVE_FEATURES,
            })
            return player

        if player is None:
            await self._send_error(None, writer, "not_helloed",
                                   "send 'hello' before anything else")
            return None

        if mtype == C_PING:
            await self._send(writer, {"t": S_PONG})
            return player

        if mtype == C_CREATE:
            if player.room:
                await self._send_error(player, writer, "already_in_room",
                                       "leave the current room first")
                return player
            if len(self.rooms)>=128:
                await self._send_error(player,writer,'rooms_full','The relay has reached its room limit; try later')
                return player
            code = generate_code()
            while code in self.rooms:
                code = generate_code()
            room = Room(code, player)
            room.native_mode = message.get("native", False)
            self.rooms[code] = room
            log.info("room %s created by pid %s", code, player.pid)
            await self._push_room(room)
            return player

        if mtype in (C_RESUME, C_REPLAY_ACK):
            room = player.room
            if room is None or not room.native_mode or not room.started:
                await self._send_error(player, writer, "not_native_room", "Resume requires a started native room")
                return player
            async with room.action_lock:
                await self._recover_native(player, writer, message)
            return player

        if mtype in (C_READY, C_ACK, C_FAULT):
            room = player.room
            if room is None or not room.native_mode:
                await self._send_error(player, writer, "not_native_room", "join a native room first")
                return player
            async with room.action_lock:
                if mtype == C_FAULT:
                    room.native_fault = "Native client stopped: " + message["message"]
                elif mtype == C_READY:
                    if room.started:
                        await self._send_error(player, writer, "already_started", "initial readiness is closed")
                        return player
                    room.ready_checkpoints[player.pid] = message["checkpoint"]
                    if room.preparing and set(room.ready_checkpoints) == set(room.players):
                        if len(set(room.ready_checkpoints.values())) != 1:
                            room.native_fault = "Initial game checkpoints differ; create a new room"
                        elif not room.native_fault and all(p.connected for p in room.players.values()):
                            room.start()
                            await self._push_room(room)
                            await self._broadcast(room, {"t": S_EVENT, "kind": "started", "turn": room.turn_pid})
                else:
                    if room.native_fault:
                        await self._send_error(player, writer, "native_stopped", room.native_fault)
                        return player
                    if message["event_seq"] != room.pending_event:
                        await self._send_error(player, writer, "stale_native_ack", "acknowledgment is not for the pending event")
                        return player
                    previous = room.native_acks.get(player.pid)
                    if (room.pending_kind == "started"
                            and message["checkpoint"] != room.ready_checkpoints.get(player.pid)):
                        room.native_fault = "Native state changed after initial readiness"
                    if previous and previous != message["checkpoint"]:
                        room.native_fault = "Client changed its native checkpoint"
                    room.native_acks[player.pid] = message["checkpoint"]
                    room.record_decision(player.pid, message.get('decision'))
                    if len(set(room.native_acks.values())) > 1:
                        room.native_fault = "Native checkpoints differ; start a new room"
                    elif set(room.native_acks) == set(room.players) and not room.native_fault:
                        room.complete_native_event(message['checkpoint'])
                await self._push_state(room)
            return player

        if mtype == C_JOIN:
            code = message["code"].upper()
            room = self.rooms.get(code)
            if room is None:
                await self._send_error(player, writer, "no_such_room",
                                       f"no room with code {code}")
                return player
            if player.room is room:
                await self._push_room(room)
                return player
            if player.room:
                await self._send_error(player, writer, "already_in_room",
                                       "leave the current room first")
                return player
            if room.started or room.preparing:
                await self._send_error(player, writer, "already_started",
                                       "that game has already started")
                return player
            try:
                room.add(player)
            except ProtocolError as exc:
                await self._send_error(player, writer, "room_full", str(exc))
                return player
            log.info("pid %s joined room %s", player.pid, code)
            await self._push_room(room)
            return player

        if mtype == C_LEAVE:
            room = player.room
            if room:
                room.remove(player)
                if room.players:
                    await self._push_room(room)
                else:
                    self.rooms.pop(room.code, None)
            await self._send(writer, {"t": S_BYE, "reason": "left_room"})
            return player

        if mtype == C_START:
            room = player.room
            if room is None:
                await self._send_error(player, writer, "no_room", "join a room first")
                return player
            if room.host_pid != player.pid:
                await self._send_error(player, writer, "not_host",
                                       "only the host can start the game")
                return player
            if room.started:
                await self._send_error(player, writer, "already_started",
                                       "the game is already running")
                return player
            if len(room.players) < 2:
                await self._send_error(player, writer, "need_players",
                                       "at least 2 players are required")
                return player
            if room.preparing:
                await self._send_error(player, writer, "preparing", "Local games are already being prepared")
                return player
            if (room.native_mode and message.get('prepare') is True and not room.native_fault
                    and all(p.connected for p in room.players.values())
                    and set(room.ready_checkpoints) != set(room.players)):
                async with room.action_lock:
                    room.preparing = True
                    await self._push_room(room)
                    await self._push_state(room)
                return player
            if room.native_mode and (set(room.ready_checkpoints) != set(room.players)
                    or len(set(room.ready_checkpoints.values())) != 1
                    or not all(p.connected for p in room.players.values()) or room.native_fault):
                await self._send_error(player, writer, "native_not_ready",
                                       "all game clients must report matching initial checkpoints")
                return player
            async with room.action_lock:
                room.start()
                await self._push_room(room)
                await self._broadcast(room, {"t": S_EVENT, "kind": "started",
                                             "turn": room.turn_pid})
                await self._push_state(room)
            return player

        if mtype == C_CHAT:
            room = player.room
            if room is None:
                await self._send_error(player, writer, "no_room", "join a room first")
                return player
            await self._broadcast(room, {"t": S_CHAT, "from": player.name,
                                         "player": player.pid,
                                         "text": message["text"]})
            return player

        if mtype == C_ACTION:
            await self._handle_action(player, writer, message)
            return player

        await self._send_error(player, writer, "unhandled", f"no handler for {mtype}")
        return player

    async def _handle_action(self, player: Player, writer: asyncio.StreamWriter,
                             message: dict) -> None:
        room = player.room
        if room is None:
            await self._send_error(player, writer, "not_playing", "no game is running in your room")
            return
        async with room.action_lock:
            await self._handle_action_locked(player, writer, message)

    async def _handle_action_locked(self, player: Player, writer: asyncio.StreamWriter,
                                    message: dict) -> None:
        room = player.room
        if room is None or not room.started:
            await self._send_error(player, writer, "not_playing",
                                   "no game is running in your room")
            return
        if room.finished:
            await self._send_error(player,writer,'game_finished','This match has finished')
            return
        if room.native_mode:
            if room.native_fault:
                await self._send_error(player, writer, "native_stopped", room.native_fault)
                return
            if room.pending_event is not None:
                await self._send_error(player, writer, "awaiting_games", "waiting for all native game copies")
                return
            if not all(p.connected and not p.recovering for p in room.players.values()):
                await self._send_error(player, writer, "awaiting_games", "a game client is disconnected")
                return
        auction = room.native_decision if room.native_mode else None
        if auction and auction['kind'] != 'turn':
            actor = list(room.players)[auction['actor_index']]
            if player.pid != actor:
                await self._send_error(player, writer, 'not_your_decision', 'Waiting for the current auction bidder')
                return
            action = message['action']
            if auction['kind']=='manage':
                changes = ('manage_build','manage_sell','manage_commit','manage_cancel_changes','manage_clear_selection')
                if (auction['pending'] and action not in changes) or (not auction['pending'] and action in ('manage_commit','manage_cancel_changes')):
                    await self._send_error(player,writer,'manage_pending','Commit or cancel pending buildings before changing properties')
                    return
                if action not in ('manage_close','manage_next','manage_previous','manage_select','manage_clear_selection','manage_mortgage','manage_unmortgage','manage_build','manage_sell','manage_commit','manage_cancel_changes'):
                    await self._send_error(player,writer,'manage_pending','Finish the current property management decision')
                    return
                await self._broadcast(room,{'t':S_EVENT,'kind':action,'by':player.pid,
                                            **({'square':message['square']} if action in ('manage_mortgage','manage_unmortgage','manage_build','manage_sell','manage_select') else {})})
                await self._push_state(room)
                return
            if auction['kind'] == 'card':
                if action != 'ack_card':
                    await self._send_error(player, writer, 'card_pending', 'Continue the current card first')
                    return
                await self._broadcast(room, {'t':S_EVENT,'kind':'ack_card','by':player.pid})
                await self._push_state(room)
                return
            if auction['kind']=='trade_target':
                if action=='trade_cancel':
                    await self._broadcast(room,{'t':S_EVENT,'kind':'trade_cancel','by':player.pid})
                    await self._push_state(room)
                    return
                if action!='trade_select' or message['target']>=len(room.players) or message['target']==auction['actor_index']:
                    await self._send_error(player,writer,'trade_target_pending','Choose another occupied trade partner')
                    return
                await self._broadcast(room,{'t':S_EVENT,'kind':'trade_select','by':player.pid,'target':message['target']})
                await self._push_state(room)
                return
            if auction['kind']=='trade':
                if action not in ('trade_offer','trade_accept','trade_cancel'):
                    await self._send_error(player,writer,'trade_pending','Finish the current trade first')
                    return
                event={'t':S_EVENT,'kind':action,'by':player.pid}
                if action=='trade_offer':event['offer']=copy.deepcopy(message['offer'])
                await self._broadcast(room,event)
                await self._push_state(room)
                return
            if action not in ('auction_bid', 'auction_pass', 'auction_withdraw'):
                await self._send_error(player, writer, 'auction_pending', 'Finish the auction decision first')
                return
            if action == 'auction_bid' and not auction['highest'] < message['amount'] <= auction['cash']:
                await self._send_error(player, writer, 'invalid_bid', 'Bid must exceed the highest bid and fit available cash')
                return
            event = {'t': S_EVENT, 'kind': action, 'by': player.pid}
            if action == 'auction_bid': event['amount'] = message['amount']
            await self._broadcast(room, event)
            await self._push_state(room)
            return
        if room.turn_pid != player.pid:
            await self._send_error(player, writer, "not_your_turn",
                                   "it is not your turn")
            return

        action = message["action"]
        if action in ('decline_property','jail_pay','jail_card','trade_open','manage_open','declare_bankruptcy') and room.native_mode:
            await self._broadcast(room, {'t': S_EVENT, 'kind': action, 'by': player.pid})
            await self._push_state(room)
            return

        if action == "buy":
            # Relay the current player's intent only. Native adapters validate
            # phase, ownership and funds; the server does not model board rules.
            await self._broadcast(room, {"t": S_EVENT, "kind": "purchase",
                                         "by": player.pid})
            await self._push_state(room)
            return

        if action == "roll":
            dice = [roll_die(), roll_die()]
            is_double = dice[0] == dice[1]
            room.rolls.append(dice)
            event = {"t": S_EVENT, "kind": "dice", "by": player.pid,
                     "values": dice, "total": sum(dice), "doubles": is_double}

            if is_double:
                room.doubles_count += 1
                event["doubles_count"] = room.doubles_count
                if room.doubles_count >= MAX_DOUBLES:
                    # Third double in a row: the game sends the player to jail.
                    event["forced_jail"] = True
                    if not room.native_mode:
                        room.advance_turn()
            else:
                room.doubles_count = 0
            await self._broadcast(room, event)
            await self._push_state(room)
            return

        if action == "end_turn":
            previous = room.turn_pid
            room.advance_turn()
            await self._broadcast(room, {"t": S_EVENT, "kind": "turn",
                                         "from": previous, "to": room.turn_pid})
            await self._push_state(room)
            return

        await self._send_error(player, writer, "unknown_action",
                               f"unsupported action {action!r}")

    async def _recover_native(self, player, writer, message):
        """Advance only a checkpoint-verified cursor; send one replay at a time."""
        room = player.room
        if room.native_fault:
            await self._send_error(player, writer, "native_stopped", room.native_fault)
            return
        seq, digest = message["event_seq"], message["checkpoint"]
        if message["t"] == C_REPLAY_ACK:
            if not player.recovering or player.recovery_seq is None or seq != player.recovery_seq + 1:
                await self._send_error(player, writer, "bad_recovery_cursor", "Replay acknowledgment is out of order")
                return
        if seq > room.event_seq:
            await self._send_error(player, writer, "bad_recovery_cursor", "Recovery cursor is ahead of the room")
            return
        expected = room.checkpoints.get(seq)
        if seq == room.pending_event:
            expected = (room.native_acks.get(player.pid)
                        or next(iter(room.native_acks.values()), None))
            if room.pending_kind == "started":
                expected = room.checkpoints[0]
        elif expected is None:
            await self._send_error(player, writer, "bad_recovery_cursor", "No verified checkpoint for that cursor")
            return
        if expected is not None and digest != expected:
            await self._send_error(player, writer, "recovery_mismatch", "Local game differs from the room checkpoint; recreate and replay the match")
            return
        if seq != room.pending_event and message.get('decision') != room.decisions.get(seq):
            await self._send_error(player, writer, 'recovery_mismatch', 'Native decision differs from the verified recovery checkpoint')
            return
        if seq == room.pending_event:
            room.native_acks[player.pid] = digest
            room.record_decision(player.pid, message.get('decision'))
            if room.native_fault:
                await self._push_state(room)
                return
            if len(set(room.native_acks.values())) > 1:
                room.native_fault = "Native checkpoints differ during recovery"
                await self._push_state(room)
                return
            if set(room.native_acks) == set(room.players):
                room.complete_native_event(digest)
        player.recovery_seq = seq
        player.recovering = seq < room.event_seq
        if player.recovering:
            event = room.event_history.get(seq + 1)
            if event is None:
                await self._send_error(player, writer, "missing_recovery_event", "Room history is incomplete")
                return
            await self._send(writer, {**copy.deepcopy(event), "replay": True})
        await self._push_state(room)

    async def _disconnect(self, player: Optional[Player], *, writer=None) -> None:
        if player is None:
            return
        if writer is not None and player.writer is not writer:
            return  # a superseded socket cannot disconnect the recovered seat
        player.connected = False
        player.writer = None
        log.info("pid %s disconnected", player.pid)
        room = player.room
        if room:
            room.last_activity = time.monotonic()
            if room.native_mode and room.started:
                player.recovering = True
                player.recovery_seq = None
                await self._push_state(room)
            # Keep the seat so the player can reconnect with their token; only
            # drop them from a room that has not started to avoid ghost seats.
            if not room.started:
                room.remove(player)
                if room.players:
                    await self._push_room(room)
                else:
                    self.rooms.pop(room.code, None)
            else:
                await self._push_room(room)

    # -- lifecycle ----------------------------------------------------------
    async def start(self, host: str = "127.0.0.1", port: int = 8500
                    ) -> asyncio.AbstractServer:
        self._server = await asyncio.start_server(self.handle, host, port)
        return self._server

    async def stop(self, timeout: float = 2.0) -> None:
        """Shut down cleanly, even with clients still attached.

        ``Server.wait_closed()`` does not return while any connection handler
        is still running, so simply calling it would hang a server that has
        live clients. Close their writers first and bound the wait.
        """
        for player in list(self.players.values()):
            player.connected = False
            if player.writer is not None:
                with contextlib.suppress(Exception):
                    player.writer.close()
                player.writer = None
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(asyncio.TimeoutError, ConnectionError,
                                     RuntimeError):
                await asyncio.wait_for(self._server.wait_closed(), timeout)
            self._server = None

    def bound_port(self) -> Optional[int]:
        if not self._server:
            return None
        for sock in self._server.sockets:
            return sock.getsockname()[1]
        return None


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Monopoly online session server")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8500)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    async def runner() -> None:
        server = SessionServer()
        await server.start(args.host, args.port)
        log.info("listening on %s:%d", args.host, args.port)
        async with server._server:  # type: ignore[union-attr]
            await server._server.serve_forever()  # type: ignore[union-attr]

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        log.info("shutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
