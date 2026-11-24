# NEON BLOCKS online server (1v1)

A small WebSocket game server. **Python 3.9+, standard library only: nothing to install.**
Rooms, matchmaking, a server-verified match engine, reconnection, expiry.

```
server/
  server.py        rooms, matchmaking, match engine, HTTP health check
  ws.py            WebSocket protocol (RFC 6455) written for this project
  game_rules.py    NEON BLOCKS rules in Python: the server replays every move with them
  tests/           end-to-end tests (real sockets, Python + Node.js clients)
  Dockerfile  render.yaml  Procfile  requirements.txt
```

## 1. Run it on your computer
```
cd server
python3 server.py                 # listens on ws://0.0.0.0:8080   (Windows: py server.py)
```
Open http://127.0.0.1:8080/health in a browser: you should see `{"status": "ok", ...}`.

In the game: **ONLINE -> SERVER ADDRESS**, type `ws://127.0.0.1:8080`, press SAVE.
Two players on one PC: in the Godot editor use **Debug -> Run Multiple Instances -> 2**, or run the game
twice. A phone on the same Wi-Fi can use `ws://<your PC's LAN address>:8080` (allow Python through the firewall).
`ws://` (no encryption) is only for testing; **for the internet always use `wss://`** (section 3).

Settings (environment variables, all optional)

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | 8080 | TCP port (hosting platforms set it for you) |
| `HOST` | 0.0.0.0 | listen address |
| `ALLOWED_ORIGINS` | (empty = any) | comma list of browser origins allowed, e.g. `https://you.itch.io`. Native apps (Android/PC) send no origin and are always allowed |
| `TLS_CERT`, `TLS_KEY` | (empty) | paths to a certificate + key: the server then speaks `wss://` itself |
| `NB_MATCH_GRACE` | 30 | seconds a disconnected player may come back during a match (then they forfeit) |
| `NB_LOBBY_GRACE` | 60 | same, in the lobby |
| `NB_ROOM_TTL` | 600 | idle lobby/finished rooms expire after this many seconds |
| `NB_COUNTDOWN` | 3 | pre-match countdown |
| `NB_MAX_ROOMS` / `NB_MAX_CONNS` / `NB_MAX_CONNS_PER_IP` | 2000 / 4000 / 30 | limits |
| `LOG_LEVEL` | INFO | DEBUG, INFO, WARNING |

## 2. Run the tests
```
cd server
python3 tests/run_all.py          # needs Node.js 22+ and openssl for the cross-client / TLS tests
```
Starts real server processes and plays real matches with several independent clients.

## 3. Put it on the internet (you need a host; nothing is deployed for you)
The server speaks plain WebSocket. Every platform below puts HTTPS/WSS in front of it automatically, so
players connect with `wss://your-address` and Godot (Android, Windows, Web) just works.

**A. Render.com (simplest, no credit card needed for the free plan)**
1. Create a free GitHub account and a new repository. Upload the **contents of this `server` folder**
   (so `server.py` is in the repository root).
2. Create a Render account -> **New -> Web Service** -> connect the repository.
3. Runtime **Python 3**. Build command: leave empty (or `echo ok`). Start command: `python3 server.py`.
   Health check path: `/health`. Plan: Free.
4. Deploy. Render shows your address, e.g. `https://neon-blocks-server.onrender.com`.
5. In the game use **`wss://neon-blocks-server.onrender.com`** (same name, `wss://` instead of `https://`).
   Free plan note: a free service goes to sleep after about 15 minutes without traffic and needs roughly a minute
   to wake up (the first connection after a break may time out; just retry). Rooms live in memory, so a sleeping or
   restarted server forgets all rooms. A paid always-on instance avoids that (see costs below).

**B. Any Docker host (Fly.io, Railway, Google Cloud Run with min-instances, ...)**
`docker build -t neon-blocks-server .` then follow the platform's "deploy a Docker image" guide.
Expose port 8080 (or let the platform set `PORT`). The platform provides the `wss://` address.

**C. Your own cheap Linux VPS + domain (most control)**
1. Rent a small VPS (1 vCPU / 512 MB is plenty), point a domain or sub-domain at it.
2. `scp -r server user@host:~/neon-server`, then run it with systemd or `nohup python3 server.py &`.
3. Install Caddy; `Caddyfile`:
   ```
   play.yourdomain.com {
       reverse_proxy 127.0.0.1:8080
   }
   ```
   Caddy gets a free certificate automatically. The game address is `wss://play.yourdomain.com`.
   (Alternative without a proxy: set `TLS_CERT` / `TLS_KEY` and `PORT=443`.)

**What you must configure** - only the server address. Either type it in the game (ONLINE -> SERVER ADDRESS)
or set `DEFAULT_SERVER_URL` in `res://core/net_config.gd` before exporting, so players need not type it.
No API keys or accounts are used anywhere.

**Costs** (check the providers' current pricing pages; these change): Render free plan 0, an always-on small
instance is a few US dollars per month; a small VPS is roughly 4-6 USD per month; a domain about 10-15 USD per year
(optional: Render/Fly give you a free sub-domain). Traffic is tiny: a few hundred bytes per move.

## 4. How it works
* Clients speak JSON text messages over one WebSocket. Every message has a type `t`.
* **Player** -> server: `hello, ping, create_room, join_room, quick_match, cancel_quick, rejoin, set_ready,
  set_duration, leave_room, move, sync, rematch`.
* **Server** -> player: `welcome, pong, error, room_joined, room_update, matchmaking, match_start, tick, move_ack,
  opponent, match_end, rejoin_ok, resync, room_closed, left`.
* Room codes: 6 characters from `ABCDEFGHJKMNPQRSTUVWXYZ23456789` (no I, L, O, 0, 1), random, unique.
* **Match**: both players ready (or quick match) -> `match_start` with the SAME 150 piece sets, countdown 3 s,
  duration 60/120/180 s. The server clock decides when the match starts and ends; the client timer is display only.
* **Moves**: a client plays instantly on its own screen and sends `move {slot, x, y}`. The server replays it with
  `game_rules.py` on its own copy of that player's board, answers `move_ack` with the verified score and tells the
  opponent. The score shown in the result is always the server's replayed score.
* **Reconnect**: after `room_joined` the client holds a secret `token`. If the connection drops, the client reconnects
  and sends `rejoin {code, token}`; the server returns the exact state (board, pieces, score, time left). The opponent
  sees "offline" and the match continues. After `NB_MATCH_GRACE` seconds without return the absent player forfeits.
* **Heartbeat**: clients send `ping` every 4 s (browsers cannot send WebSocket ping frames); a connection silent for
  15 s is dropped. Idle lobby/finished rooms expire after 10 minutes.
* **Rematch**: both press REMATCH -> a new match with new pieces. Quick match pairs two waiting players of the same length.

## 5. Anti-cheat: what is protected and what is NOT
Protected (implemented and tested):
* **Scores cannot be forged.** The client never sends a score. The server replays every move with the real rules
  (placement, line clears, combos, perfect clear). Illegal moves, used or non-existing pieces, moves outside the match
  window and moves after the end are rejected. The server returns its authoritative state after a rejected move.
* **Pieces cannot be chosen.** The server generates the sequence; a claimed piece that is not in the player's current
  set is refused. Both players get the identical sequence (fair).
* **Rate and volume limits**: a minimum 0.12 s between moves, at most 600 moves per match, a message flood limit,
  8 KB message size, per-IP and global connection limits. 8 refused moves in a match disqualify the player.
* **Seats cannot be hijacked** (rejoin needs the secret token); optional browser origin allow list.

NOT protected (be honest about it):
* **Automation / solver bots** and screen-reading helpers look like a fast, perfect human; the server cannot tell.
* **Collusion** (two friends cooperating) and **multi-accounting** are not detectable: there are no accounts.
* The full piece sequence is sent at match start (it is the same for both players, but a modified client can plan ahead).
* Anyone who knows the server address can connect: there is no login. Use `ALLOWED_ORIGINS` for web builds and add
  authentication before a public launch with prizes or rankings.
* State lives in memory: a server restart ends all rooms. One process = one machine (no horizontal scaling).
* Time authority is the server's, but very high latency naturally disadvantages a player.
