#!/usr/bin/env python3
"""NEON BLOCKS online server (1v1). Standard library only - no pip install needed.

Run:   python3 server.py                      (listens on PORT, default 8080)
Plain  ws:// is served here; in production put a TLS terminator in front (Render, Fly.io,
Railway, Caddy, nginx ...) to get wss://, or set TLS_CERT / TLS_KEY to serve wss:// directly.

All settings are environment variables - see README.md.
"""
import asyncio
import json
import logging
import os
import secrets
import signal
import ssl
import sys
import time
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import game_rules as rules
import ws as wsproto

VERSION = "1.0"
ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"      # no I, L, O, 0, 1 (easy to read and type)
CODE_LENGTH = 6
log = logging.getLogger("neonblocks")


def env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


class Config:
    def __init__(self):
        self.host = os.environ.get("HOST", "0.0.0.0")
        self.port = int(os.environ.get("PORT", "8080"))
        self.allowed_origins = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()]
        self.tls_cert = os.environ.get("TLS_CERT", "")
        self.tls_key = os.environ.get("TLS_KEY", "")
        self.test_mode = os.environ.get("NB_TEST_MODE", "") == "1"
        self.countdown = env_float("NB_COUNTDOWN", 3.0)
        self.end_grace = env_float("NB_END_GRACE", 0.6)
        self.match_grace = env_float("NB_MATCH_GRACE", 30.0)      # seconds a disconnected player may return mid-match
        self.lobby_grace = env_float("NB_LOBBY_GRACE", 60.0)
        self.room_ttl = env_float("NB_ROOM_TTL", 600.0)           # idle lobby / finished rooms expire
        self.heartbeat_timeout = env_float("NB_HEARTBEAT_TIMEOUT", 15.0)
        self.queue_timeout = env_float("NB_QUEUE_TIMEOUT", 120.0)
        self.min_move_interval = env_float("NB_MIN_MOVE_INTERVAL", 0.12)
        self.max_moves = int(env_float("NB_MAX_MOVES", 600))
        self.max_violations = int(env_float("NB_MAX_VIOLATIONS", 8))
        self.max_rooms = int(env_float("NB_MAX_ROOMS", 2000))
        self.max_conns = int(env_float("NB_MAX_CONNS", 4000))
        self.max_conns_per_ip = int(env_float("NB_MAX_CONNS_PER_IP", 30))
        self.durations = {60, 120, 180} | ({3, 5, 8} if self.test_mode else set())


class ClientError(Exception):
    def __init__(self, code, message=""):
        super().__init__(code)
        self.code = code
        self.message = message


def sanitize_name(value):
    text = "".join(ch for ch in str(value or "") if ch.isprintable() and ch not in "<>\"'\\").strip()
    return text[:12] or "Player"


class Conn:
    def __init__(self, ws, ip):
        self.ws = ws
        self.ip = ip
        self.id = secrets.token_hex(4)
        self.name = "Player"
        self.pid = ""
        self.room = None
        self.idx = -1
        self.queued_duration = None
        self.last_seen = time.monotonic()
        self.bucket = 40.0
        self.bucket_at = time.monotonic()
        self.strikes = 0

    async def send(self, message):
        try:
            await self.ws.send_text(json.dumps(message, separators=(",", ":")))
        except Exception:
            pass


class Player:
    def __init__(self, pid, name):
        self.pid = pid
        self.name = name
        self.token = secrets.token_hex(16)
        self.conn = None
        self.ready = False
        self.rematch = False
        self.connected = True
        self.disconnected_at = None
        self.state = None            # rules.PlayerState during/after a match


class Room:
    def __init__(self, code, duration):
        self.code = code
        self.duration = duration
        self.players = [None, None]
        self.state = "lobby"          # lobby | countdown | playing | finished
        self.sets = []
        self.seed = 0
        self.starts_at = 0.0
        self.ends_at = 0.0
        self.task = None
        self.result = None
        self.last_activity = time.monotonic()
        self.match_number = 0

    def touch(self):
        self.last_activity = time.monotonic()

    def count(self):
        return sum(1 for p in self.players if p is not None)


class Hub:
    def __init__(self, cfg):
        self.cfg = cfg
        self.rooms = {}
        self.conns = set()
        self.ip_counts = {}
        self.queue = {}               # duration -> [Conn]
        self.handlers = {
            "hello": self.h_hello, "ping": self.h_ping, "create_room": self.h_create_room,
            "join_room": self.h_join_room, "quick_match": self.h_quick_match, "cancel_quick": self.h_cancel_quick,
            "rejoin": self.h_rejoin, "set_ready": self.h_set_ready, "set_duration": self.h_set_duration,
            "leave_room": self.h_leave_room, "move": self.h_move, "sync": self.h_sync, "rematch": self.h_rematch,
        }

    # ------------------------------------------------------------------ views
    def room_view(self, room, idx):
        players = []
        for p in room.players:
            if p is None:
                players.append(None)
            else:
                players.append({"name": p.name, "ready": p.ready, "connected": p.connected, "rematch": p.rematch})
        return {"code": room.code, "state": room.state, "duration": room.duration, "you": idx, "players": players}

    async def broadcast_room(self, room):
        for idx, p in enumerate(room.players):
            if p is not None and p.conn is not None:
                await p.conn.send({"t": "room_update", "room": self.room_view(room, idx)})

    async def send_to(self, room, idx, message):
        p = room.players[idx]
        if p is not None and p.conn is not None:
            await p.conn.send(message)

    def resync_payload(self, room, idx):
        me = room.players[idx]
        other = room.players[1 - idx]
        now = time.monotonic()
        payload = {
            "state": room.state, "duration": room.duration, "sets": room.sets, "you": idx,
            "names": [p.name if p else "" for p in room.players],
            "countdown_ms": max(0, int((room.starts_at - now) * 1000)) if room.state == "countdown" else 0,
            "remaining_ms": max(0, int((room.ends_at - now) * 1000)) if room.state == "playing" else 0,
            "result": room.result if room.state == "finished" else None,
            "me": me.state.snapshot() if me and me.state else None,
        }
        if other and other.state:
            payload["opp"] = {"score": other.state.score, "combo": other.state.combo, "moves": other.state.moves,
                              "finished": other.state.finished}
        return payload

    # ------------------------------------------------------------------ helpers
    def new_code(self):
        for _ in range(100):
            code = "".join(secrets.choice(ALPHABET) for _ in range(CODE_LENGTH))
            if code not in self.rooms:
                return code
        raise ClientError("server_busy", "could not create a room code")

    def check_duration(self, value):
        try:
            duration = int(value)
        except (TypeError, ValueError):
            duration = 120
        if duration not in self.cfg.durations:
            raise ClientError("bad_duration", "unsupported match length")
        return duration

    def identify(self, conn, msg):
        conn.name = sanitize_name(msg.get("name", conn.name))
        conn.pid = str(msg.get("pid", conn.pid))[:32]

    def attach(self, room, idx, conn):
        player = Player(conn.pid, conn.name)
        player.conn = conn
        room.players[idx] = player
        conn.room = room
        conn.idx = idx
        room.touch()
        return player

    # ------------------------------------------------------------------ handlers
    async def h_hello(self, conn, msg):
        self.identify(conn, msg)
        await conn.send({"t": "welcome", "version": VERSION, "server_time": time.time(),
                         "durations": sorted(self.cfg.durations)})

    async def h_ping(self, conn, msg):
        await conn.send({"t": "pong", "ts": msg.get("ts", 0), "st": time.time()})

    async def h_create_room(self, conn, msg):
        if conn.room is not None:
            raise ClientError("already_in_room", "leave your current room first")
        if len(self.rooms) >= self.cfg.max_rooms:
            raise ClientError("server_busy", "too many rooms right now")
        self.identify(conn, msg)
        self._leave_queue(conn)
        room = Room(self.new_code(), self.check_duration(msg.get("duration", 120)))
        self.rooms[room.code] = room
        player = self.attach(room, 0, conn)
        log.info("room %s created", room.code)
        await conn.send({"t": "room_joined", "you": 0, "token": player.token, "room": self.room_view(room, 0)})

    async def h_join_room(self, conn, msg):
        if conn.room is not None:
            raise ClientError("already_in_room", "leave your current room first")
        self.identify(conn, msg)
        code = str(msg.get("code", "")).strip().upper()
        if len(code) != CODE_LENGTH or any(ch not in ALPHABET for ch in code):
            raise ClientError("bad_code", "a room code has 6 letters and digits")
        room = self.rooms.get(code)
        if room is None:
            raise ClientError("room_not_found", "no room with this code")
        free = [i for i, p in enumerate(room.players) if p is None]
        if not free:
            raise ClientError("room_full", "this room already has two players")
        if room.state != "lobby":
            raise ClientError("room_busy", "a match is in progress")
        self._leave_queue(conn)
        idx = free[0]
        player = self.attach(room, idx, conn)
        log.info("room %s joined", room.code)
        await conn.send({"t": "room_joined", "you": idx, "token": player.token, "room": self.room_view(room, idx)})
        await self.broadcast_room(room)

    async def h_quick_match(self, conn, msg):
        if conn.room is not None:
            raise ClientError("already_in_room", "leave your current room first")
        self.identify(conn, msg)
        duration = self.check_duration(msg.get("duration", 120))
        self._leave_queue(conn)
        waiting = self.queue.setdefault(duration, [])
        while waiting:
            other = waiting.pop(0)
            if other is conn or other.room is not None or other.ws.closed:
                continue
            other.queued_duration = None
            room = Room(self.new_code(), duration)
            self.rooms[room.code] = room
            first = self.attach(room, 0, other)
            second = self.attach(room, 1, conn)
            first.ready = second.ready = True
            log.info("quick match room %s", room.code)
            await other.send({"t": "room_joined", "you": 0, "token": first.token, "room": self.room_view(room, 0), "quick": True})
            await conn.send({"t": "room_joined", "you": 1, "token": second.token, "room": self.room_view(room, 1), "quick": True})
            await self.maybe_start(room)
            return
        waiting.append(conn)
        conn.queued_duration = duration
        conn.queued_at = time.monotonic()
        await conn.send({"t": "matchmaking", "state": "searching", "duration": duration})

    async def h_cancel_quick(self, conn, msg):
        self._leave_queue(conn)
        await conn.send({"t": "matchmaking", "state": "cancelled"})

    def _leave_queue(self, conn):
        if conn.queued_duration is not None:
            waiting = self.queue.get(conn.queued_duration, [])
            if conn in waiting:
                waiting.remove(conn)
            conn.queued_duration = None

    async def h_rejoin(self, conn, msg):
        self.identify(conn, msg)
        code = str(msg.get("code", "")).strip().upper()
        token = str(msg.get("token", ""))
        room = self.rooms.get(code)
        idx = -1
        if room is not None:
            for i, p in enumerate(room.players):
                if p is not None and secrets.compare_digest(p.token, token):
                    idx = i
        if room is None or idx < 0:
            raise ClientError("room_not_found", "this room no longer exists")
        player = room.players[idx]
        old = player.conn
        if old is not None and old is not conn:
            old.room = None
            asyncio.ensure_future(old.ws.close(1000, "replaced"))
        player.conn = conn
        player.connected = True
        player.disconnected_at = None
        conn.room = room
        conn.idx = idx
        room.touch()
        log.info("room %s player %d rejoined", room.code, idx)
        payload = {"t": "rejoin_ok", "you": idx, "token": player.token, "room": self.room_view(room, idx)}
        if room.state != "lobby":
            payload["match"] = self.resync_payload(room, idx)
        await conn.send(payload)
        await self.broadcast_room(room)

    def _require_room(self, conn):
        if conn.room is None:
            raise ClientError("not_in_room", "you are not in a room")
        conn.room.touch()
        return conn.room, conn.idx

    async def h_set_ready(self, conn, msg):
        room, idx = self._require_room(conn)
        if room.state != "lobby":
            raise ClientError("bad_state", "the match already started")
        room.players[idx].ready = bool(msg.get("ready", False))
        await self.broadcast_room(room)
        await self.maybe_start(room)

    async def h_set_duration(self, conn, msg):
        room, idx = self._require_room(conn)
        if idx != 0:
            raise ClientError("not_host", "only the host can change the match length")
        if room.state != "lobby":
            raise ClientError("bad_state", "the match already started")
        room.duration = self.check_duration(msg.get("duration"))
        for p in room.players:
            if p is not None:
                p.ready = False
        await self.broadcast_room(room)

    async def h_leave_room(self, conn, msg):
        room, idx = self._require_room(conn)
        await self.remove_player(room, idx, "left")
        await conn.send({"t": "left"})

    async def h_sync(self, conn, msg):
        room, idx = self._require_room(conn)
        if room.state == "lobby":
            raise ClientError("bad_state", "no match to sync")
        await conn.send({"t": "resync", **self.resync_payload(room, idx)})

    async def h_rematch(self, conn, msg):
        room, idx = self._require_room(conn)
        if room.state != "finished":
            raise ClientError("bad_state", "the match is not finished")
        other = room.players[1 - idx]
        if other is None:
            raise ClientError("no_opponent", "your opponent left the room")
        room.players[idx].rematch = True
        await self.broadcast_room(room)
        if all(p is not None and p.rematch and p.connected for p in room.players):
            for p in room.players:
                p.ready = True
            room.state = "lobby"
            await self.maybe_start(room)

    # ------------------------------------------------------------------ match engine
    async def maybe_start(self, room):
        if room.state != "lobby":
            return
        if all(p is not None and p.ready and p.connected for p in room.players):
            await self.start_match(room)

    async def start_match(self, room):
        room.state = "countdown"
        room.match_number += 1
        room.seed = secrets.randbits(31)
        room.sets = rules.generate_sets(room.seed)
        room.result = None
        room.starts_at = time.monotonic() + self.cfg.countdown
        room.ends_at = room.starts_at + room.duration
        for p in room.players:
            p.state = rules.PlayerState(room.sets)
            p.rematch = False
        names = [p.name for p in room.players]
        log.info("room %s match #%d started (%ds)", room.code, room.match_number, room.duration)
        for idx in range(2):
            await self.send_to(room, idx, {
                "t": "match_start", "code": room.code, "you": idx, "names": names, "duration": room.duration,
                "countdown_ms": int(self.cfg.countdown * 1000), "sets": room.sets})
        await self.broadcast_room(room)
        room.task = asyncio.ensure_future(self.run_match(room))

    async def run_match(self, room):
        try:
            await asyncio.sleep(self.cfg.countdown)
            if room.state != "countdown":
                return
            room.state = "playing"
            await self.broadcast_room(room)
            while room.state == "playing":
                now = time.monotonic()
                remaining = room.ends_at + self.cfg.end_grace - now
                if remaining <= 0:
                    break
                tick = {"t": "tick", "remaining_ms": max(0, int((room.ends_at - now) * 1000)),
                        "scores": [p.state.score for p in room.players],
                        "finished": [p.state.finished for p in room.players]}
                for idx in range(2):
                    await self.send_to(room, idx, tick)
                await asyncio.sleep(min(1.0, remaining))
            if room.state == "playing":
                await self.end_match(room, "time")
        except asyncio.CancelledError:
            pass

    async def end_match(self, room, reason, winner=None):
        if room.state == "finished":
            return
        room.state = "finished"
        current = asyncio.current_task()
        if room.task is not None and room.task is not current and not room.task.done():
            room.task.cancel()
        scores = [p.state.score if p and p.state else 0 for p in room.players]
        if winner is None:
            winner = 0 if scores[0] > scores[1] else (1 if scores[1] > scores[0] else -1)
        stats = []
        for p in room.players:
            st = p.state if p else None
            stats.append({"moves": st.moves if st else 0, "lines": st.lines if st else 0,
                          "max_combo": st.max_combo if st else 0, "perfect": st.perfect_clears if st else 0})
        room.result = {"winner": winner, "scores": scores, "reason": reason,
                       "names": [p.name if p else "" for p in room.players],
                       "stats": stats, "duration": room.duration}
        for p in room.players:
            if p is not None:
                p.ready = False
                p.rematch = False
        room.touch()
        log.info("room %s match over: %s %s", room.code, scores, reason)
        for idx in range(2):
            await self.send_to(room, idx, {"t": "match_end", "result": room.result})
        await self.broadcast_room(room)

    async def h_move(self, conn, msg):
        room, idx = self._require_room(conn)
        if room.state != "playing":
            raise ClientError("not_playing", "the match is not running")
        player = room.players[idx]
        state = player.state
        now = time.monotonic()
        seq = msg.get("seq", 0)
        if now > room.ends_at + self.cfg.end_grace:
            raise ClientError("match_over", "time is up")
        reason = None
        result = None
        try:
            if now - state.last_move_at < self.cfg.min_move_interval:
                raise rules.MoveError("too_fast")
            if state.moves >= self.cfg.max_moves:
                raise rules.MoveError("limit")
            result = state.apply_move(int(msg["slot"]), int(msg["x"]), int(msg["y"]))
        except rules.MoveError as err:
            reason = err.reason
        except (KeyError, TypeError, ValueError):
            reason = "bad_move"
        if reason is not None:
            if reason not in ("too_fast", "finished"):
                state.violations += 1
            await conn.send({"t": "move_ack", "seq": seq, "ok": False, "reason": reason, "state": state.snapshot()})
            if state.violations >= self.cfg.max_violations:
                log.warning("room %s player %d disqualified", room.code, idx)
                await self.end_match(room, "disqualified", winner=1 - idx)
            return
        state.last_move_at = now
        await conn.send({"t": "move_ack", "seq": seq, "ok": True, "score": state.score, "combo": state.combo,
                         "moves": state.moves, "lines": result["lines"], "gained": result["gained"],
                         "finished": state.finished})
        await self.send_to(room, 1 - idx, {"t": "opponent", "score": state.score, "combo": state.combo,
                                           "moves": state.moves, "finished": state.finished,
                                           "gained": result["gained"], "lines": result["lines"]})
        if all(p.state.finished for p in room.players):
            await self.end_match(room, "both_done")

    # ------------------------------------------------------------------ leaving / cleanup
    async def remove_player(self, room, idx, reason):
        player = room.players[idx]
        if player is None:
            return
        if room.state in ("countdown", "playing") and room.players[1 - idx] is not None:
            await self.end_match(room, "forfeit", winner=1 - idx)
        if player.conn is not None:
            player.conn.room = None
            player.conn.idx = -1
        room.players[idx] = None
        other = room.players[1 - idx]
        if other is None:
            await self.close_room(room, reason)
            return
        if room.state in ("finished", "countdown", "playing"):
            pass
        if idx == 0:                                   # the guest becomes the host
            room.players[0], room.players[1] = other, None
            if other.conn is not None:
                other.conn.idx = 0
        if room.state == "finished" or room.state == "lobby":
            room.state = "lobby"
            room.result = None
            for p in room.players:
                if p is not None:
                    p.ready = False
                    p.rematch = False
        room.touch()
        await self.broadcast_room(room)

    async def close_room(self, room, reason):
        if room.task is not None and not room.task.done():
            room.task.cancel()
        self.rooms.pop(room.code, None)
        for p in room.players:
            if p is not None and p.conn is not None:
                p.conn.room = None
                p.conn.idx = -1
                await p.conn.send({"t": "room_closed", "reason": reason})
        room.players = [None, None]
        log.info("room %s closed (%s)", room.code, reason)

    async def on_disconnect(self, conn):
        self.conns.discard(conn)
        self._leave_queue(conn)
        room = conn.room
        if room is None:
            return
        player = room.players[conn.idx] if 0 <= conn.idx < 2 else None
        if player is None or player.conn is not conn:
            return
        player.conn = None
        player.connected = False
        player.disconnected_at = time.monotonic()
        log.info("room %s player %d disconnected", room.code, conn.idx)
        await self.broadcast_room(room)

    async def reaper(self):
        while True:
            await asyncio.sleep(1.0)
            try:
                await self._reap()
            except Exception:
                log.exception("reaper error")

    async def _reap(self):
        now = time.monotonic()
        for conn in list(self.conns):
            if now - conn.last_seen > self.cfg.heartbeat_timeout:
                asyncio.ensure_future(conn.ws.close(1001, "timeout"))
        for duration, waiting in list(self.queue.items()):
            for conn in list(waiting):
                if now - getattr(conn, "queued_at", now) > self.cfg.queue_timeout:
                    waiting.remove(conn)
                    conn.queued_duration = None
                    await conn.send({"t": "matchmaking", "state": "timeout"})
        for room in list(self.rooms.values()):
            if room.state in ("lobby", "finished") and now - room.last_activity > self.cfg.room_ttl:
                await self.close_room(room, "expired")
                continue
            for idx in (0, 1):
                p = room.players[idx]
                if p is None or p.connected or p.disconnected_at is None:
                    continue
                grace = self.cfg.match_grace if room.state in ("countdown", "playing") else self.cfg.lobby_grace
                if now - p.disconnected_at > grace:
                    log.info("room %s player %d did not return", room.code, idx)
                    await self.remove_player(room, idx, "abandoned")
                    break
            if room.code in self.rooms and room.count() == 0:
                await self.close_room(room, "empty")

    # ------------------------------------------------------------------ connection handling
    async def handle_client(self, reader, writer):
        peer = writer.get_extra_info("peername")
        ip = peer[0] if peer else "?"
        try:
            request = await wsproto.read_http_request(reader)
            if request is None:
                return
            method, path, headers = request
            if not wsproto.is_upgrade(headers):
                if method == "GET" and path.split("?")[0] in ("/", "/health", "/healthz"):
                    body = json.dumps({"status": "ok", "name": "neon-blocks-server", "version": VERSION,
                                       "rooms": len(self.rooms), "connections": len(self.conns)})
                    writer.write(wsproto.http_response(200, body, "application/json"))
                else:
                    writer.write(wsproto.http_response(404, "not found"))
                await writer.drain()
                return
            origin = headers.get("origin", "")
            if self.cfg.allowed_origins and origin and origin not in self.cfg.allowed_origins:
                writer.write(wsproto.http_response(403, "origin not allowed"))
                await writer.drain()
                return
            if len(self.conns) >= self.cfg.max_conns or self.ip_counts.get(ip, 0) >= self.cfg.max_conns_per_ip:
                writer.write(wsproto.http_response(503, "too many connections"))
                await writer.drain()
                return
            response = wsproto.handshake_response(headers)
            if response is None:
                writer.write(wsproto.http_response(400, "bad websocket handshake"))
                await writer.drain()
                return
            writer.write(response)
            await writer.drain()
            await self.serve(wsproto.WebSocket(reader, writer, max_message=8192), ip)
        except Exception:
            log.exception("connection error")
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def serve(self, ws, ip):
        conn = Conn(ws, ip)
        self.conns.add(conn)
        self.ip_counts[ip] = self.ip_counts.get(ip, 0) + 1
        try:
            await conn.send({"t": "welcome", "version": VERSION, "server_time": time.time(),
                             "durations": sorted(self.cfg.durations)})
            while True:
                text = await ws.recv()
                if text is None:
                    break
                now = time.monotonic()
                conn.last_seen = now
                conn.bucket = min(40.0, conn.bucket + (now - conn.bucket_at) * 25.0)
                conn.bucket_at = now
                if conn.bucket < 1.0:
                    conn.strikes += 1
                    if conn.strikes > 100:
                        await ws.close(1008, "rate limit")
                        break
                    await conn.send({"t": "error", "code": "rate_limited", "message": "slow down"})
                    continue
                conn.bucket -= 1.0
                try:
                    msg = json.loads(text)
                    if not isinstance(msg, dict):
                        raise ValueError("not an object")
                    handler = self.handlers.get(msg.get("t"))
                    if handler is None:
                        raise ClientError("unknown_message", "unknown message type")
                    await handler(conn, msg)
                except ClientError as err:
                    await conn.send({"t": "error", "code": err.code, "message": err.message})
                except ValueError:
                    await conn.send({"t": "error", "code": "bad_json", "message": "invalid message"})
        finally:
            self.ip_counts[ip] = max(0, self.ip_counts.get(ip, 1) - 1)
            await self.on_disconnect(conn)


async def run_server(cfg, ready=None):
    hub = Hub(cfg)
    ssl_context = None
    if cfg.tls_cert and cfg.tls_key:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(cfg.tls_cert, cfg.tls_key)
    server = await asyncio.start_server(hub.handle_client, cfg.host, cfg.port, ssl=ssl_context, limit=65536)
    port = server.sockets[0].getsockname()[1]
    scheme = "wss" if ssl_context else "ws"
    print(f"LISTENING {port} {scheme}", flush=True)
    log.info("NEON BLOCKS server %s listening on %s://%s:%d", VERSION, scheme, cfg.host, port)
    reaper = asyncio.ensure_future(hub.reaper())
    if ready is not None:
        ready.set_result((hub, server, port))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        await stop.wait()
    finally:
        reaper.cancel()
        server.close()


def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    cfg = Config()
    if len(sys.argv) > 2 and sys.argv[1] == "--port":
        cfg.port = int(sys.argv[2])
    try:
        asyncio.run(run_server(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
