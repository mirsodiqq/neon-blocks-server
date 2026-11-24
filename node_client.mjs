// Independent client #2: uses Node's built-in WebSocket (a different implementation than the
// Python one and than the server's). It plays a match by tracking its own board in plain JS.
// usage: node node_client.mjs <url> <create|join> <code|-> <name> <moves>
const [url, mode, code, name, movesArg] = process.argv.slice(2);
const MOVES = parseInt(movesArg || "4");
const G = 10;
const ws = new WebSocket(url);
let board = Array(G * G).fill(-1);
let sets = [], setIndex = 0, used = [false, false, false], me = 0, seq = 0, sent = 0, acksOk = 0, acksBad = 0;
let myScore = 0, oppScore = 0, roomCode = "", lastResult = null;
const out = (o) => console.log(JSON.stringify(o));
const send = (o) => ws.send(JSON.stringify(o));

function canPlace(cells, x, y) {
  return cells.every(([cx, cy]) => { const px = x + cx, py = y + cy;
    return px >= 0 && px < G && py >= 0 && py < G && board[py * G + px] === -1; });
}
function place(piece, x, y) {
  for (const [cx, cy] of piece.c) board[(y + cy) * G + x + cx] = piece.k;
  const rows = [], cols = [];
  for (let r = 0; r < G; r++) if (board.slice(r * G, r * G + G).every(v => v !== -1)) rows.push(r);
  for (let c = 0; c < G; c++) if ([...Array(G).keys()].every(r => board[r * G + c] !== -1)) cols.push(c);
  for (const r of rows) for (let c = 0; c < G; c++) board[r * G + c] = -1;
  for (const c of cols) for (let r = 0; r < G; r++) board[r * G + c] = -1;
}
function pickMove() {
  const set = sets[setIndex];
  for (let slot = 0; slot < 3; slot++) {
    if (used[slot]) continue;
    for (let y = G - 1; y >= 0; y--) for (let x = 0; x < G; x++)
      if (canPlace(set[slot].c, x, y)) return { slot, x, y };
  }
  return null;
}
async function play() {
  for (let i = 0; i < MOVES; i++) {
    const mv = pickMove(); if (!mv) break;
    const piece = sets[setIndex][mv.slot];
    place(piece, mv.x, mv.y); used[mv.slot] = true;
    if (used.every(Boolean)) { setIndex++; used = [false, false, false]; }
    send({ t: "move", seq: ++seq, ...mv }); sent++;
    await new Promise(r => setTimeout(r, 200));
  }
}
ws.onopen = () => {
  send({ t: "hello", name, pid: "node-" + name });
  if (mode === "create") send({ t: "create_room", name, duration: 5 });
  else send({ t: "join_room", name, code });
};
ws.onmessage = (ev) => {
  const m = JSON.parse(ev.data);
  if (m.t === "room_joined") { me = m.you; roomCode = m.room.code; out({ ev: "joined", you: me, code: roomCode });
    send({ t: "set_ready", ready: true }); }
  else if (m.t === "match_start") { sets = m.sets; out({ ev: "match_start", sets: sets.length, countdown: m.countdown_ms });
    setTimeout(play, m.countdown_ms + 100); }
  else if (m.t === "move_ack") { if (m.ok) { acksOk++; myScore = m.score; } else { acksBad++; out({ ev: "bad_ack", reason: m.reason }); } }
  else if (m.t === "opponent") { oppScore = m.score; }
  else if (m.t === "match_end") { lastResult = m.result;
    out({ ev: "match_end", result: m.result, acksOk, acksBad, myScore, oppScore, sent });
    setTimeout(() => { ws.close(); process.exit(0); }, 200); }
  else if (m.t === "error") { out({ ev: "error", code: m.code }); }
};
ws.onerror = (e) => { out({ ev: "socket_error", message: String(e.message || e) }); process.exit(2); };
setTimeout(() => { out({ ev: "timeout" }); process.exit(3); }, 25000);
