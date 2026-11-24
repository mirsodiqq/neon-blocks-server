#!/usr/bin/env python3
"""End-to-end tests for the NEON BLOCKS server: real server processes, real TCP/TLS sockets,
several independent clients (Python and Node.js). Run:  python3 tests/run_all.py
Needs Python 3.9+ and (for the cross-client tests) Node.js 22+. No pip packages."""
import asyncio
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER_DIR = os.path.dirname(HERE)
sys.path.insert(0, SERVER_DIR)
sys.path.insert(0, HERE)
import game_rules as rules          # noqa: E402
from wsclient import Client          # noqa: E402

passed = failed = 0


def check(condition, label):
    global passed, failed
    if condition:
        passed += 1
        print("PASS ", label, flush=True)
    else:
        failed += 1
        print("FAIL ", label, flush=True)


class ServerProc:
    def __init__(self, **env):
        base = dict(os.environ, NB_TEST_MODE="1", NB_COUNTDOWN="0.3", NB_END_GRACE="0.4", LOG_LEVEL="WARNING")
        base.update({k: str(v) for k, v in env.items()})
        self.proc = subprocess.Popen([sys.executable, os.path.join(SERVER_DIR, "server.py"), "--port", "0"],
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=base)
        line = self.proc.stdout.readline().strip()
        _, port, scheme = line.split()
        self.port, self.scheme = int(port), scheme

    @property
    def url(self):
        return f"{self.scheme}://127.0.0.1:{self.port}"

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.proc.kill()


async def new_client(server, label, name=None, origin=None):
    client = Client(label)
    await client.connect("127.0.0.1", server.port, use_tls=(server.scheme == "wss"), origin=origin)
    await client.wait("welcome", 3)
    if name:
        await client.send({"t": "hello", "name": name, "pid": label})
        await client.wait("welcome", 3)
    return client


async def create_room(client, name, duration=5):
    await client.send({"t": "create_room", "name": name, "duration": duration})
    return await client.wait("room_joined", 3)


async def join_room(client, name, code):
    await client.send({"t": "join_room", "name": name, "code": code})
    return await client.wait("room_joined", 3)


async def start_match(a, b, a_name="Alice", b_name="Bob", duration=5):
    joined = await create_room(a, a_name, duration)
    code = joined["room"]["code"]
    joined_b = await join_room(b, b_name, code)
    await a.wait("room_update", 3, where=lambda m: m["room"]["players"][1] is not None)
    await a.send({"t": "set_ready", "ready": True})
    await b.send({"t": "set_ready", "ready": True})
    ms_a = await a.wait("match_start", 3)
    ms_b = await b.wait("match_start", 3)
    await asyncio.sleep(0.45)                         # countdown (0.3 s) is over
    return code, joined, joined_b, ms_a, ms_b


def find_move(state):
    current = state.current_set()
    for y in range(rules.GRID - 1, -1, -1):
        for x in range(rules.GRID):
            for slot in range(3):
                if not state.used[slot] and state.can_place(current[slot]["c"], x, y):
                    return slot, x, y
    return None


async def play(client, state, moves, seq_start=1, gap=0.15):
    """Plays `moves` legal moves, mirrors them locally and checks every ack against the local replay."""
    ok = mismatches = 0
    for i in range(moves):
        move = find_move(state)
        if move is None:
            break
        slot, x, y = move
        await client.send({"t": "move", "seq": seq_start + i, "slot": slot, "x": x, "y": y})
        state.apply_move(slot, x, y)
        ack = await client.wait("move_ack", 3)
        if ack["ok"]:
            ok += 1
            if ack["score"] != state.score:
                mismatches += 1
        else:
            mismatches += 1
        await asyncio.sleep(gap)
    return ok, mismatches


# ============================================================================ scenarios

async def test_http():
    s = ServerProc()
    try:
        import urllib.request
        body = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{s.port}/health", timeout=3).read())
        check(body["status"] == "ok", "HTTP /health answers (hosting platforms use this)")
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{s.port}/nope", timeout=3)
            check(False, "unknown HTTP path gives 404")
        except Exception as err:
            check("404" in str(err), "unknown HTTP path gives 404")
    finally:
        s.stop()


async def test_full_match_and_winner():
    s = ServerProc()
    a = b = None
    try:
        a, b = await new_client(s, "A"), await new_client(s, "B")
        joined = await create_room(a, "Alice")
        code = joined["room"]["code"]
        check(len(code) == 6 and all(ch in "ABCDEFGHJKMNPQRSTUVWXYZ23456789" for ch in code), "TEST 1: A creates a room with a valid 6-character code")
        check(joined["room"]["state"] == "lobby" and joined["room"]["players"][0]["name"] == "Alice" and joined["you"] == 0, "room starts in lobby with the host's name")
        joined_b = await join_room(b, "Bob", code.lower())      # lower case must work
        upd = await a.wait("room_update", 3, where=lambda m: m["room"]["players"][1] is not None)
        names = [p["name"] for p in upd["room"]["players"]]
        check(names == ["Alice", "Bob"] and joined_b["you"] == 1, "TEST 2: B joins with the code (case-insensitive); both names are shown")
        await a.send({"t": "set_ready", "ready": True})
        upd = await b.wait("room_update", 3, where=lambda m: m["room"]["players"][0]["ready"])
        check(upd["room"]["players"][0]["ready"] and not upd["room"]["players"][1]["ready"], "TEST 3a: ready status of one player is visible to the other")
        await b.send({"t": "set_ready", "ready": True})
        ms_a = await a.wait("match_start", 3)
        ms_b = await b.wait("match_start", 3)
        check(ms_a["sets"] == ms_b["sets"] and len(ms_a["sets"]) == rules.SET_COUNT, "TEST 3b/4: match starts only when both are ready; both get the SAME 150 piece sets")
        check(ms_a["duration"] == ms_b["duration"] == 5 and ms_a["countdown_ms"] == 300 and ms_a["you"] == 0 and ms_b["you"] == 1, "same duration and countdown for both players")
        await asyncio.sleep(0.45)
        state_a, state_b = rules.PlayerState(ms_a["sets"]), rules.PlayerState(ms_b["sets"])
        (ok_a, bad_a), (ok_b, bad_b) = await asyncio.gather(play(a, state_a, 7), play(b, state_b, 2))
        check(ok_a == 7 and ok_b == 2 and bad_a == 0 and bad_b == 0, "TEST 5: every move is acknowledged and the server score equals the local replay score")
        opp = b.drain("opponent")
        check(len(opp) == 7 and opp[-1]["score"] == state_a.score, "live score: B received 7 opponent updates; the last equals A's score")
        tick = await a.wait("tick", 3)
        check(0 < tick["remaining_ms"] <= 5000 and len(tick["scores"]) == 2, "match timer ticks arrive with remaining time and both scores")
        end_a = await a.wait("match_end", 10)
        end_b = await b.wait("match_end", 10)
        result = end_a["result"]
        check(end_a["result"] == end_b["result"], "both players receive the identical result")
        check(result["scores"] == [state_a.score, state_b.score] and state_a.score > state_b.score, "TEST 5/6: final scores are the server's replayed scores")
        check(result["winner"] == 0 and result["reason"] == "time", "TEST 6: winner = higher score; the match ended automatically when the timer reached zero")
        # moves after the end are refused
        await a.send({"t": "move", "seq": 99, "slot": 0, "x": 0, "y": 0})
        err = await a.wait("error", 3)
        check(err["code"] == "not_playing", "moves after the match are rejected")
        # rematch
        a.drain("tick")
        b.drain("tick")
        await a.send({"t": "rematch"})
        upd = await b.wait("room_update", 3, where=lambda m: m["room"]["players"][0]["rematch"])
        check(upd["room"]["state"] == "finished", "TEST 8a: rematch request of one player is shown to the other")
        await b.send({"t": "rematch"})
        ms2 = await a.wait("match_start", 3)
        await b.wait("match_start", 3)
        tick2 = await a.wait("tick", 3, where=lambda m: True)
        check(ms2["sets"] != ms_a["sets"] and tick2["scores"] == [0, 0], "TEST 8b: rematch starts a NEW match (new pieces, scores reset)")
    finally:
        for c in (a, b):
            if c:
                await c.close()
        s.stop()


async def test_message_shapes():
    """The Godot client reads exactly these fields; this pins them so client and server cannot drift apart."""
    s = ServerProc()
    a = b = None
    try:
        a, b = await new_client(s, "A"), await new_client(s, "B")
        joined = await create_room(a, "Alice")
        need = lambda d, keys: all(k in d for k in keys)
        check(need(joined, ["you", "token", "room"]) and need(joined["room"], ["code", "state", "duration", "you", "players"])
              and need(joined["room"]["players"][0], ["name", "ready", "connected", "rematch"]), "room_joined carries you, token and a complete room view")
        await join_room(b, "Bob", joined["room"]["code"])
        await a.send({"t": "set_ready", "ready": True})
        await b.send({"t": "set_ready", "ready": True})
        ms = await a.wait("match_start", 3)
        check(need(ms, ["sets", "you", "names", "duration", "countdown_ms", "code"]) and all(need(p, ["n", "k", "c"]) for p in ms["sets"][0]),
              "match_start carries sets, you, names, duration, countdown_ms; each piece has n / k / c")
        await asyncio.sleep(0.45)
        state = rules.PlayerState(ms["sets"])
        move = find_move(state)
        await a.send({"t": "move", "seq": 1, "slot": move[0], "x": move[1], "y": move[2]})
        ack = await a.wait("move_ack", 3)
        opp = await b.wait("opponent", 3)
        check(need(ack, ["seq", "ok", "score", "combo", "moves", "finished"]) and need(opp, ["score", "combo", "moves", "finished", "gained", "lines"]),
              "move_ack and opponent carry the fields the game reads")
        await a.send({"t": "sync"})
        rs = await a.wait("resync", 3)
        check(need(rs, ["state", "sets", "you", "names", "duration", "remaining_ms", "me", "opp"]) and need(rs["me"], ["score", "combo", "moves", "set_index", "used", "board", "finished"]) and len(rs["me"]["board"]) == 100,
              "resync carries the full authoritative state (100-cell board, set_index, used flags, remaining time)")
        tick = await a.wait("tick", 3)
        check(need(tick, ["remaining_ms", "scores", "finished"]), "tick carries remaining_ms, scores, finished")
    finally:
        for c in (a, b):
            if c:
                await c.close()
        s.stop()


async def test_draw():
    s = ServerProc()
    a = b = None
    try:
        a, b = await new_client(s, "A"), await new_client(s, "B")
        _, _, _, ms_a, ms_b = await start_match(a, b)
        sa, sb = rules.PlayerState(ms_a["sets"]), rules.PlayerState(ms_b["sets"])
        await asyncio.gather(play(a, sa, 4), play(b, sb, 4))
        end = (await a.wait("match_end", 10))["result"]
        check(sa.score == sb.score and sa.score > 0 and end["scores"][0] == end["scores"][1], "TEST 7: identical play gives equal non-zero scores")
        check(end["winner"] == -1, "TEST 7: equal scores are announced as a draw (winner = -1)")
    finally:
        for c in (a, b):
            if c:
                await c.close()
        s.stop()


async def test_errors():
    s = ServerProc()
    a = b = c = d = None
    try:
        a, b, c, d = [await new_client(s, n) for n in "ABCD"]
        await b.send({"t": "join_room", "name": "B", "code": "ABC"})
        check((await b.wait("error", 3))["code"] == "bad_code", "invalid code format -> bad_code")
        await b.send({"t": "join_room", "name": "B", "code": "ZZZZZZ"})
        check((await b.wait("error", 3))["code"] == "room_not_found", "unknown room -> room_not_found")
        joined = await create_room(a, "A")
        code = joined["room"]["code"]
        await join_room(b, "B", code)
        await c.send({"t": "join_room", "name": "C", "code": code})
        check((await c.wait("error", 3))["code"] == "room_full", "third player -> room_full")
        await a.send({"t": "create_room", "name": "A"})
        check((await a.wait("error", 3))["code"] == "already_in_room", "creating a second room while in one is refused")
        await b.send({"t": "set_duration", "duration": 60})
        check((await b.wait("error", 3))["code"] == "not_host", "only the host can change the match length")
        await d.send({"t": "create_room", "name": "D", "duration": 77})
        check((await d.wait("error", 3))["code"] == "bad_duration", "unsupported duration refused")
        await d.send_raw_text("this is not json")
        check((await d.wait("error", 3))["code"] == "bad_json", "malformed JSON -> bad_json (connection stays open)")
        await d.send({"t": "fly_to_moon"})
        check((await d.wait("error", 3))["code"] == "unknown_message", "unknown message type refused")
        await d.send({"t": "move", "slot": 0, "x": 0, "y": 0})
        check((await d.wait("error", 3))["code"] == "not_in_room", "game messages outside a room refused")
        await d.send_raw_text("x" * 9000)
        await asyncio.sleep(0.5)
        check(d.closed and d.close_code == 1009, "oversized message -> connection closed with code 1009")
        # illegal moves
        await a.send({"t": "set_ready", "ready": True})
        await b.send({"t": "set_ready", "ready": True})
        ms = await a.wait("match_start", 3)
        await a.send({"t": "move", "seq": 1, "slot": 0, "x": 0, "y": 0})
        check((await a.wait("error", 3))["code"] == "not_playing", "a move during the countdown is refused")
        await asyncio.sleep(0.45)
        await a.send({"t": "move", "seq": 2, "slot": 0, "x": 99, "y": 99})
        ack = await a.wait("move_ack", 3)
        check(ack["ok"] is False and ack["reason"] == "illegal" and ack["state"]["score"] == 0, "illegal placement is rejected and the server returns the authoritative state")
        await a.send({"t": "move", "seq": 3, "slot": 7, "x": 0, "y": 0})
        check((await a.wait("move_ack", 3))["reason"] == "bad_slot", "invalid slot is rejected")
        await a.send({"t": "move", "seq": 4, "slot": "zero"})
        check((await a.wait("move_ack", 3))["reason"] == "bad_move", "garbled move is rejected")
        state = rules.PlayerState(ms["sets"])
        slot, x, y = find_move(state)
        await a.send({"t": "move", "seq": 5, "slot": slot, "x": x, "y": y})
        await a.send({"t": "move", "seq": 6, "slot": slot, "x": x, "y": y})     # instantly again
        first, second = await a.wait("move_ack", 3), await a.wait("move_ack", 3)
        check(first["ok"] and second["ok"] is False and second["reason"] == "too_fast", "moves faster than 0.12 s are refused (rate limit)")
        # spam
        for _ in range(120):
            await d_spam(c)
        await asyncio.sleep(0.2)
        check(any(m.get("code") == "rate_limited" for m in c.messages), "message flood -> rate_limited")
    finally:
        for x_ in (a, b, c, d):
            if x_:
                await x_.close()
        s.stop()


async def d_spam(client):
    try:
        await client.send({"t": "ping", "ts": 1})
    except Exception:
        pass


async def test_disqualification():
    s = ServerProc(NB_MAX_VIOLATIONS=3)
    a = b = None
    try:
        a, b = await new_client(s, "A"), await new_client(s, "B")
        await start_match(a, b)
        for i in range(3):
            await a.send({"t": "move", "seq": i, "slot": 0, "x": 99, "y": 0})
            await a.wait("move_ack", 3)
            await asyncio.sleep(0.15)
        end = (await b.wait("match_end", 5))["result"]
        check(end["reason"] == "disqualified" and end["winner"] == 1, "repeated illegal moves disqualify the cheater (opponent wins)")
    finally:
        for c in (a, b):
            if c:
                await c.close()
        s.stop()


async def test_reconnect():
    s = ServerProc(NB_MATCH_GRACE=20)
    a = b = b2 = None
    try:
        a, b = await new_client(s, "A"), await new_client(s, "B")
        code, _, joined_b, ms_a, ms_b = await start_match(a, b, duration=8)
        token = joined_b["token"]
        sa, sb = rules.PlayerState(ms_a["sets"]), rules.PlayerState(ms_b["sets"])
        await asyncio.gather(play(a, sa, 3), play(b, sb, 3))
        await b.abort()                                          # B's internet dies (no close frame)
        upd = await a.wait("room_update", 3, where=lambda m: m["room"]["players"][1]["connected"] is False)
        check(upd["room"]["state"] == "playing", "TEST 9a: opponent's disconnect is announced; the match keeps running")
        await play(a, sa, 2)
        b2 = await new_client(s, "B2")
        await b2.send({"t": "rejoin", "code": code, "token": token, "name": "Bob"})
        rj = await b2.wait("rejoin_ok", 3)
        me = rj["match"]["me"]
        check(rj["you"] == 1 and me["score"] == sb.score and me["board"] == sb.board and me["set_index"] == sb.set_index and me["used"] == sb.used,
              "TEST 9b: B reconnects with the token and receives its exact authoritative state (score, board, pieces)")
        check(rj["match"]["opp"]["score"] == sa.score and rj["match"]["remaining_ms"] > 0, "rejoin also carries the opponent's score and the remaining time")
        await a.wait("room_update", 3, where=lambda m: m["room"]["players"][1]["connected"] is True)
        ok, bad = await play(b2, sb, 3, seq_start=50)
        check(ok == 3 and bad == 0, "TEST 9c: B keeps playing after the reconnect; moves are accepted")
        end = (await a.wait("match_end", 10))["result"]
        check(end["scores"] == [sa.score, sb.score] and end["reason"] == "time", "result after the reconnect contains the moves from before AND after the disconnect")
        # a stranger cannot hijack the seat
        c = await new_client(s, "C")
        await c.send({"t": "rejoin", "code": code, "token": "0" * 32})
        check((await c.wait("error", 3))["code"] == "room_not_found", "rejoin with a wrong token is refused")
        await c.close()
    finally:
        for x in (a, b, b2):
            if x:
                await x.close()
        s.stop()


async def test_forfeit():
    s = ServerProc(NB_MATCH_GRACE=1.0)
    a = b = None
    try:
        a, b = await new_client(s, "A"), await new_client(s, "B")
        await start_match(a, b, duration=8)
        await b.abort()
        end = (await a.wait("match_end", 6))["result"]
        check(end["reason"] == "forfeit" and end["winner"] == 0, "TEST 9d: opponent who does not return in time forfeits; the remaining player wins")
    finally:
        for c in (a, b):
            if c:
                await c.close()
        s.stop()


async def test_leave_and_lobby():
    s = ServerProc()
    a = b = c = None
    try:
        a, b, c = [await new_client(s, n) for n in "ABC"]
        code = (await create_room(a, "Alice"))["room"]["code"]
        await join_room(b, "Bob", code)
        await a.send({"t": "leave_room"})
        await a.wait("left", 3)
        upd = await b.wait("room_update", 3, where=lambda m: m["room"]["players"][1] is None)
        check(upd["room"]["you"] == 0 and upd["room"]["players"][0]["name"] == "Bob", "host leaves the lobby: the guest becomes host")
        await join_room(c, "Cleo", code)
        check(True, "a free seat can be taken by a new player")
        await b.send({"t": "set_ready", "ready": True})
        await c.send({"t": "set_ready", "ready": True})
        await b.wait("match_start", 3)
        await c.wait("match_start", 3)
        await asyncio.sleep(0.45)
        await c.send({"t": "leave_room"})
        end = (await b.wait("match_end", 3))["result"]
        check(end["reason"] == "forfeit" and end["winner"] == 0, "leaving during a match = forfeit")
    finally:
        for x in (a, b, c):
            if x:
                await x.close()
        s.stop()


async def test_expiry():
    s = ServerProc(NB_ROOM_TTL=1.5)
    a = b = None
    try:
        a, b = await new_client(s, "A"), await new_client(s, "B")
        code = (await create_room(a, "Alice"))["room"]["code"]
        closed = await a.wait("room_closed", 6)
        check(closed["reason"] == "expired", "TEST 7: an idle room expires and its owner is told")
        await b.send({"t": "join_room", "name": "Bob", "code": code})
        check((await b.wait("error", 3))["code"] == "room_not_found", "joining an expired room -> room_not_found")
    finally:
        for c in (a, b):
            if c:
                await c.close()
        s.stop()


async def test_quick_match():
    s = ServerProc()
    a = b = c = None
    try:
        a, b, c = [await new_client(s, n) for n in "ABC"]
        await a.send({"t": "quick_match", "name": "Alice", "duration": 5})
        check((await a.wait("matchmaking", 3))["state"] == "searching", "quick match: first player is searching")
        await c.send({"t": "quick_match", "name": "Cleo", "duration": 60})
        await c.wait("matchmaking", 3)
        await c.send({"t": "cancel_quick"})
        check((await c.wait("matchmaking", 3))["state"] == "cancelled", "quick match can be cancelled")
        await b.send({"t": "quick_match", "name": "Bob", "duration": 5})
        ja = await a.wait("room_joined", 3)
        jb = await b.wait("room_joined", 3)
        check(ja["room"]["code"] == jb["room"]["code"] and ja["you"] == 0 and jb["you"] == 1, "quick match pairs two players with the same duration into one room")
        ms_a = await a.wait("match_start", 3)
        ms_b = await b.wait("match_start", 3)
        check(ms_a["names"] == ["Alice", "Bob"] and ms_b["sets"] == ms_a["sets"], "quick match starts automatically (both auto-ready)")
    finally:
        for x in (a, b, c):
            if x:
                await x.close()
        s.stop()


async def test_heartbeat():
    s = ServerProc(NB_HEARTBEAT_TIMEOUT=2)
    quiet = alive = None
    try:
        quiet, alive = await new_client(s, "quiet"), await new_client(s, "alive")
        for _ in range(8):
            await alive.send({"t": "ping", "ts": 1})
            await asyncio.sleep(0.5)
        pong = await alive.wait("pong", 2)
        check(quiet.closed and quiet.close_code == 1001, "silent connection is dropped by the heartbeat timeout")
        check(not alive.closed and pong["t"] == "pong", "a client that pings stays connected")
    finally:
        for x in (quiet, alive):
            if x:
                await x.close()
        s.stop()


async def test_origin():
    s = ServerProc(ALLOWED_ORIGINS="https://good.example")
    try:
        bad = Client("bad")
        try:
            await bad.connect("127.0.0.1", s.port, origin="https://evil.example")
            check(False, "browser origin not on the allow list is refused")
        except ConnectionError as err:
            check("403" in str(err), "browser origin not on the allow list is refused (403)")
        good = await Client("good").connect("127.0.0.1", s.port, origin="https://good.example")
        await good.wait("welcome", 3)
        native = await Client("native").connect("127.0.0.1", s.port)       # Godot Android/PC send no Origin
        await native.wait("welcome", 3)
        check(True, "allowed origin and origin-less (native) clients connect")
        await good.close()
        await native.close()
    finally:
        s.stop()


async def run_node(url, mode, code, name, moves):
    proc = await asyncio.create_subprocess_exec(
        "node", os.path.join(HERE, "node_client.mjs"), url, mode, code, name, str(moves),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env=dict(os.environ, NODE_TLS_REJECT_UNAUTHORIZED="0", NODE_NO_WARNINGS="1"))
    return proc


async def read_node(proc, timeout=30):
    events = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = await asyncio.wait_for(proc.stdout.readline(), timeout=max(0.1, deadline - time.monotonic()))
        if not line:
            break
        try:
            events.append(json.loads(line))
        except ValueError:
            pass
    await proc.wait()
    return events


async def test_cross_client(secure):
    cert_dir = tempfile.mkdtemp()
    env = {}
    if secure:
        key, crt = os.path.join(cert_dir, "k.pem"), os.path.join(cert_dir, "c.pem")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", crt,
                        "-days", "1", "-subj", "/CN=localhost"], check=True, capture_output=True)
        env = {"TLS_CERT": crt, "TLS_KEY": key}
    s = ServerProc(**env)
    label = "wss:// (TLS)" if secure else "ws://"
    a = None
    try:
        a = await new_client(s, "A")
        code = (await create_room(a, "PyPlayer"))["room"]["code"]
        node = await run_node(s.url, "join", code, "NodeBob", 4)         # a completely different implementation
        await a.wait("room_update", 5, where=lambda m: m["room"]["players"][1] is not None)
        await a.send({"t": "set_ready", "ready": True})
        ms = await a.wait("match_start", 8)
        await asyncio.sleep(0.45)
        sa = rules.PlayerState(ms["sets"])
        await play(a, sa, 7)
        end = (await a.wait("match_end", 10))["result"]
        events = await read_node(node)
        ends = [e for e in events if e.get("ev") == "match_end"]
        check(len(ends) == 1, f"[{label}] Node.js client (independent WebSocket stack) completed the match")
        if ends:
            e = ends[0]
            check(e["acksBad"] == 0 and e["acksOk"] == 4, f"[{label}] all 4 Node moves were legal on the server (its own board tracking agrees with the server)")
            check(e["result"] == end and e["result"]["scores"][0] == sa.score and e["result"]["scores"][1] == e["myScore"], f"[{label}] both clients saw the same server-verified result; scores match each side's view")
            check(e["oppScore"] == sa.score, f"[{label}] Node client received the Python player's live score")
    finally:
        if a:
            await a.close()
        s.stop()
        shutil.rmtree(cert_dir, ignore_errors=True)


async def test_tls_python():
    cert_dir = tempfile.mkdtemp()
    key, crt = os.path.join(cert_dir, "k.pem"), os.path.join(cert_dir, "c.pem")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", crt,
                    "-days", "1", "-subj", "/CN=localhost"], check=True, capture_output=True)
    s = ServerProc(TLS_CERT=crt, TLS_KEY=key)
    a = b = None
    try:
        check(s.scheme == "wss", "server serves wss:// when TLS_CERT / TLS_KEY are set")
        a, b = await new_client(s, "A"), await new_client(s, "B")
        _, _, _, ms_a, ms_b = await start_match(a, b)
        check(ms_a["sets"] == ms_b["sets"], "two Python clients complete lobby + match start over TLS")
    finally:
        for c in (a, b):
            if c:
                await c.close()
        s.stop()
        shutil.rmtree(cert_dir, ignore_errors=True)


def test_rules_parity():
    """Independent re-implementation (written differently, floats like the Godot game) vs game_rules."""
    def reference(sets, moves):
        board = {}
        score = combo = 0
        for slot, x, y, set_index in moves:
            piece = sets[set_index][slot]
            for cx, cy in piece["c"]:
                board[(x + cx, y + cy)] = piece["k"]
            rows = [r for r in range(10) if all((c, r) in board for c in range(10))]
            cols = [c for c in range(10) if all((c, r) in board for r in range(10))]
            n = len(rows) + len(cols)
            gained = len(piece["c"])
            if n:
                combo += 1
                for r in rows:
                    for c in range(10):
                        board.pop((c, r), None)
                for c in cols:
                    for r in range(10):
                        board.pop((c, r), None)
                mult = min(1.0 + (combo - 1) * 0.5, 5.0)
                gained += int(round(50 * n * n * mult))
                if not board:
                    gained += 300
            else:
                combo = 0
            score += gained
        return score
    rng = random.Random(7)
    games = bad = total_moves = 0
    for g in range(120):
        sets = rules.generate_sets(1000 + g)
        st = rules.PlayerState(sets)
        history = []
        while not st.finished and len(history) < 400:
            options = [(s_, x, y) for s_ in range(3) if not st.used[s_]
                       for y in range(10) for x in range(10) if st.can_place(st.current_set()[s_]["c"], x, y)]
            if not options:
                break
            slot, x, y = rng.choice(options)
            history.append((slot, x, y, st.set_index))
            st.apply_move(slot, x, y)
        games += 1
        total_moves += len(history)
        if reference(sets, history) != st.score:
            bad += 1
    check(bad == 0, f"server scoring == independent reference scoring on {games} random games ({total_moves} moves)")
    # determinism + all pieces valid
    check(rules.generate_sets(5) == rules.generate_sets(5) and rules.generate_sets(5) != rules.generate_sets(6), "piece sequence depends only on the seed")
    sets = rules.generate_sets(99)
    check(all(len(p["c"]) <= 5 for s_ in sets for p in s_) and all(any(len(p["c"]) <= 4 for p in s_) for s_ in sets),
          "every set has only small pieces (<=5 blocks) and at least one piece of <=4 blocks")


async def main():
    only = sys.argv[1] if len(sys.argv) > 1 else ""
    tests = [("rules", None), ("http", test_http), ("match", test_full_match_and_winner), ("shapes", test_message_shapes), ("draw", test_draw),
             ("errors", test_errors), ("dq", test_disqualification), ("reconnect", test_reconnect),
             ("forfeit", test_forfeit), ("leave", test_leave_and_lobby), ("expiry", test_expiry),
             ("quick", test_quick_match), ("heartbeat", test_heartbeat), ("origin", test_origin),
             ("tls", test_tls_python), ("node", lambda: test_cross_client(False)), ("nodetls", lambda: test_cross_client(True))]
    for name, fn in tests:
        if only and only != name:
            continue
        print(f"--- {name}", flush=True)
        try:
            if fn is None:
                test_rules_parity()
            else:
                await asyncio.wait_for(fn(), 90)
        except Exception as err:
            global failed
            failed += 1
            print("FAIL ", f"scenario '{name}' crashed: {type(err).__name__}: {err}", flush=True)
    print(f"---- RESULT: {passed} passed, {failed} failed ----")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
