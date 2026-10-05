"""Wire protocol for the Monopoly online session layer.

Framing
-------
Every message is a 4-byte big-endian length prefix followed by UTF-8 JSON. JSON
keeps the protocol inspectable with ordinary tools, and a length prefix makes
TCP framing unambiguous — no delimiter escaping, no partial-message guessing.

Division of responsibility
--------------------------
This layer owns everything that must be *authoritative* so that no player can
forge it:

* room membership and invite codes
* whose turn it is
* the dice values (rolled with a CSPRNG on the server, never on a client)
* sequencing, so clients can detect a missed update

It deliberately does NOT own the board rules (rent, auctions, building). Those
belong to the game itself, and the mod mirrors them — see README.
"""

from __future__ import annotations

import json
import struct
from typing import Any, Iterable

PROTOCOL_VERSION = 1

# 4-byte big-endian length prefix.
HEADER = struct.Struct(">I")
MAX_FRAME = 1 << 20  # 1 MiB: nothing we send should approach this

# Invite-code alphabet with visually ambiguous characters removed (no I, O, 0, 1).
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
CODE_LENGTH = 6

# --- client -> server ------------------------------------------------------
C_HELLO = "hello"            # {name, version, token?}
C_CREATE = "create_room"
C_JOIN = "join_room"         # {code}
C_LEAVE = "leave_room"
C_START = "start"            # host only
C_ACTION = "action"          # {action, ...}
C_CHAT = "chat"              # {text}
C_PING = "ping"
C_READY = "native_ready"    # {checkpoint}: observed initial fields hash
C_ACK = "native_ack"        # {event_seq, checkpoint}: settled native receipt
C_FAULT = "native_fault"    # {message}: stop room after uncertain execution
C_RESUME = "native_resume"  # {event_seq, checkpoint}: verified recovery cursor
C_REPLAY_ACK = "native_replay_ack"

# --- server -> client ------------------------------------------------------
S_WELCOME = "welcome"        # {player_id, token, protocol}
S_ROOM = "room"              # {code, host, players, started, turn}
S_EVENT = "event"            # {kind, ...}
S_STATE = "state"            # {seq, turn, doubles, rolls}
S_CHAT = "chat"              # {from, text}
S_ERROR = "error"            # {code, message}
S_PONG = "pong"
S_BYE = "bye"                # {reason}

CLIENT_TYPES = {C_HELLO, C_CREATE, C_JOIN, C_LEAVE, C_START, C_ACTION, C_CHAT, C_PING,
                C_READY, C_ACK, C_FAULT, C_RESUME, C_REPLAY_ACK}
SERVER_TYPES = {S_WELCOME, S_ROOM, S_EVENT, S_STATE, S_CHAT, S_ERROR, S_PONG, S_BYE}

# Required fields per message type, kept *per direction*.
#
# This has to be two tables rather than one: 'chat' is deliberately the same
# wire string in both directions (a client sends {text}, the server relays
# {from, text}), so a single dict keyed by message type would silently have one
# direction's requirements overwrite the other's.
CLIENT_REQUIRED: dict[str, tuple[str, ...]] = {
    C_HELLO: ("name",),
    C_CREATE: (),
    C_JOIN: ("code",),
    C_LEAVE: (),
    C_START: (),
    C_ACTION: ("action",),
    C_CHAT: ("text",),
    C_PING: (),
    C_READY: ("checkpoint",),
    C_ACK: ("event_seq", "checkpoint"),
    C_FAULT: ("message",),
    C_RESUME: ("event_seq", "checkpoint"),
    C_REPLAY_ACK: ("event_seq", "checkpoint"),
}

SERVER_REQUIRED: dict[str, tuple[str, ...]] = {
    S_WELCOME: ("player_id", "protocol"),
    S_ROOM: ("code", "players"),
    S_EVENT: ("kind",),
    S_STATE: ("seq",),
    S_CHAT: ("from", "text"),
    S_ERROR: ("code", "message"),
    S_PONG: (),
    S_BYE: ("reason",),
}

# Flat view for introspection only — never use this for validation.
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {**CLIENT_REQUIRED, **SERVER_REQUIRED}

MAX_NAME_LENGTH = 24
MAX_CHAT_LENGTH = 400


class ProtocolError(Exception):
    """Malformed or unacceptable message on the wire."""


def encode(message: dict) -> bytes:
    """Serialise one message to a length-prefixed frame."""
    body = json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(body) > MAX_FRAME:
        raise ProtocolError(f"frame too large: {len(body)} bytes")
    return HEADER.pack(len(body)) + body


def decode_frame(frame: bytes) -> dict:
    """Deserialise one complete frame (without the length prefix)."""
    try:
        message = json.loads(frame.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"invalid JSON frame: {exc}") from exc
    if not isinstance(message, dict):
        raise ProtocolError("frame must decode to a JSON object")
    return message


def validate(message: dict, expect: str = "client") -> dict:
    """Check a decoded message against the schema. Returns it for chaining."""
    mtype = message.get("t")
    if not isinstance(mtype, str):
        raise ProtocolError("message is missing a string 't' field")
    allowed = CLIENT_TYPES if expect == "client" else SERVER_TYPES
    if mtype not in allowed:
        raise ProtocolError(f"unexpected message type {mtype!r} for a {expect}")
    table = CLIENT_REQUIRED if expect == "client" else SERVER_REQUIRED
    for field in table.get(mtype, ()):
        if field not in message:
            raise ProtocolError(f"{mtype!r} requires field {field!r}")

    if mtype == C_HELLO:
        name = message["name"]
        if not isinstance(name, str) or not name.strip():
            raise ProtocolError("'name' must be a non-empty string")
        if len(name) > MAX_NAME_LENGTH:
            raise ProtocolError(f"'name' longer than {MAX_NAME_LENGTH} chars")
        if "token" in message and (not isinstance(message["token"], str)
                                    or not 1 <= len(message["token"]) <= 128):
            raise ProtocolError("'token' must be a non-empty string of at most 128 chars")
        if "version" in message and message["version"] != PROTOCOL_VERSION:
            raise ProtocolError(
                f"protocol mismatch: client {message['version']} vs server "
                f"{PROTOCOL_VERSION}")
    elif mtype == C_JOIN:
        code = message["code"]
        if not isinstance(code, str) or len(code) != CODE_LENGTH:
            raise ProtocolError(f"'code' must be {CODE_LENGTH} characters")
        if any(c not in CODE_ALPHABET for c in code.upper()):
            raise ProtocolError("'code' contains characters not in the alphabet")
    elif mtype == C_CHAT:
        text = message["text"]
        if not isinstance(text, str) or not text.strip():
            raise ProtocolError("'text' must be a non-empty string")
        if len(text) > MAX_CHAT_LENGTH:
            raise ProtocolError(f"'text' longer than {MAX_CHAT_LENGTH} chars")
    elif mtype == C_ACTION:
        if not isinstance(message["action"], str):
            raise ProtocolError("'action' must be a string")
        if message['action'] == 'auction_bid' and (type(message.get('amount')) is not int
                                                 or not 1 <= message['amount'] < (1 << 31)):
            raise ProtocolError('Auction bid requires a positive integer amount')
        if message['action']=='trade_offer':validate_trade_offer(message.get('offer'))
        if message['action']=='trade_select' and (type(message.get('target')) is not int or not 0<=message['target']<4):
            raise ProtocolError('Trade partner requires a seat index')
        if message['action'] in ('manage_mortgage','manage_unmortgage','manage_build','manage_sell','manage_select') and (type(message.get('square')) is not int or not 0<=message['square']<40):
            raise ProtocolError('Mortgage choice requires a property square')
    elif mtype == C_START:
        if 'prepare' in message and type(message['prepare']) is not bool:
            raise ProtocolError("'prepare' must be a boolean")
    elif mtype == C_CREATE:
        if "native" in message and type(message["native"]) is not bool:
            raise ProtocolError("'native' must be a boolean")
    elif mtype in (C_READY, C_ACK, C_RESUME, C_REPLAY_ACK):
        checkpoint = message["checkpoint"]
        if (not isinstance(checkpoint, str) or len(checkpoint) != 64
                or any(ch not in "0123456789abcdef" for ch in checkpoint)):
            raise ProtocolError("'checkpoint' must be a lowercase SHA-256 digest")
        if mtype != C_READY and (type(message["event_seq"]) is not int
                                or message["event_seq"] < (0 if mtype == C_RESUME else 1)):
            raise ProtocolError("Invalid native recovery/event sequence")
        decision = message.get('decision')
        if decision is not None:
            if isinstance(decision,dict) and decision.get('kind')=='turn' and 'active' in decision:
                active=decision['active']
                if (set(decision)!={'kind','actor_index','active'} or type(decision['actor_index']) is not int
                        or not isinstance(active,list) or not 1<=len(active)<=4
                        or any(type(index) is not int or not 0<=index<4 for index in active)
                        or active!=sorted(set(active)) or decision['actor_index'] not in active):
                    raise ProtocolError('Invalid native surviving-seat report')
                return message
            if isinstance(decision,dict) and decision.get('kind')=='manage':
                if (set(decision)!={'kind','actor_index','property','pending','cost'} or type(decision['actor_index']) is not int
                        or not 0<=decision['actor_index']<4 or type(decision['property']) is not int or not 0<=decision['property']<40
                        or type(decision['pending']) is not bool or type(decision['cost']) is not int or not -(1<<31)<decision['cost']<(1<<31)):
                    raise ProtocolError('Invalid native management decision')
                return message
            if isinstance(decision,dict) and decision.get('kind')=='trade':
                required={'kind','actor_index','first','second','side','cash','properties','cards'}
                if (set(decision)!=required or any(type(decision[k]) is not int for k in ('actor_index','first','second','side'))
                        or any(not 0<=decision[k]<4 for k in ('actor_index','first','second'))
                        or decision['first']==decision['second'] or decision['side'] not in (0,1)
                        or decision['actor_index']!=(decision['first'],decision['second'])[decision['side']]):
                    raise ProtocolError('Invalid native trade decision')
                validate_trade_offer({k:decision[k] for k in ('cash','properties','cards')})
                return message
            if isinstance(decision,dict) and decision.get('kind') in ('card','turn','trade_target','winner'):
                if (set(decision) != {'kind','actor_index'} or type(decision['actor_index']) is not int
                        or not 0 <= decision['actor_index'] < 4):
                    raise ProtocolError('Invalid native card decision')
                return message
            required = {'kind','actor_index','highest','cash','property','winner_index','round','bids'}
            if (not isinstance(decision, dict) or set(decision) != required
                    or decision['kind'] != 'auction'
                    or any(type(decision[k]) is not int for k in required - {'kind','bids'})
                    or not 0 <= decision['actor_index'] < 4
                    or not 0 <= decision['property'] < 40
                    or not -1 <= decision['winner_index'] < 4
                    or not 0 <= decision['highest'] < (1 << 31)
                    or not 0 <= decision['cash'] < (1 << 31)
                    or not -1 <= decision['round'] < 32768
                    or not isinstance(decision['bids'], list) or len(decision['bids']) != 4
                    or any(type(v) is not int or not -1 <= v < (1 << 31) for v in decision['bids'])):
                raise ProtocolError('Invalid native auction decision')
    elif mtype == C_FAULT:
        if not isinstance(message["message"], str) or not 1 <= len(message["message"]) <= 200:
            raise ProtocolError("'message' must be between 1 and 200 characters")
    return message


def validate_trade_offer(offer):
    if (not isinstance(offer,dict) or set(offer)!={'cash','properties','cards'}
            or type(offer['cash']) is not int or not -(1<<31)<offer['cash']<(1<<31)
            or not isinstance(offer['properties'],list) or len(offer['properties'])>40
            or any(type(p) is not int or not 0<=p<40 for p in offer['properties'])
            or len(set(offer['properties']))!=len(offer['properties'])
            or not isinstance(offer['cards'],list) or len(offer['cards'])>2
            or any(type(p) is not int or p not in (0,1) for p in offer['cards'])
            or len(set(offer['cards']))!=len(offer['cards'])):
        raise ProtocolError('Invalid normalized trade offer')
    return offer

class FrameReader:
    """Incremental de-framer for a blocking or asyncio byte stream.

    Feed it arbitrary chunks; it yields complete messages. Holding the buffer in
    one place means neither the server nor the tests have to reimplement the
    partial-read dance.
    """

    def __init__(self) -> None:
        self.buffer = bytearray()

    def feed(self, chunk: bytes) -> list[dict]:
        self.buffer.extend(chunk)
        out: list[dict] = []
        while True:
            if len(self.buffer) < HEADER.size:
                return out
            (length,) = HEADER.unpack_from(self.buffer, 0)
            if length > MAX_FRAME:
                raise ProtocolError(f"declared frame length {length} exceeds limit")
            if len(self.buffer) < HEADER.size + length:
                return out
            body = bytes(self.buffer[HEADER.size:HEADER.size + length])
            del self.buffer[:HEADER.size + length]
            out.append(decode_frame(body))


def describe(message: dict) -> str:
    """Short human-readable summary, for logs."""
    mtype = message.get("t", "?")
    extras = {k: v for k, v in message.items() if k != "t"}
    return f"{mtype}({extras})" if extras else mtype
