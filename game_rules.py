"""NEON BLOCKS rules, ported to Python. The server REPLAYS every move with these rules, so a
client can never claim a score it did not earn. Keep the numbers in sync with the Godot game:
  placement: 1 point per block
  clear:     50 * lines^2 * multiplier, multiplier = min(1 + 0.5 * (combo - 1), 5)   (combo counts consecutive clears)
  perfect clear (board empty after a clear): +300
"""
import random

GRID = 10
SET_SIZE = 3
SET_COUNT = 150            # pieces sets pre-generated for a match
PERFECT_CLEAR_BONUS = 300

BASES = [
    ("dot", ["X"], 0.5), ("domino", ["XX"], 1.0), ("line3", ["XXX"], 1.0), ("line4", ["XXXX"], 0.8),
    ("line5", ["XXXXX"], 0.5), ("corner3", ["XX", "X."], 1.0), ("square2", ["XX", "XX"], 1.0),
    ("square3", ["XXX", "XXX", "XXX"], 0.4), ("rect2x3", ["XXX", "XXX"], 0.7), ("L4", ["X.", "X.", "XX"], 1.0),
    ("J4", [".X", ".X", "XX"], 1.0), ("T4", ["XXX", ".X."], 1.0), ("S4", [".XX", "XX."], 0.8),
    ("Z4", ["XX.", ".XX"], 0.8), ("corner5", ["X..", "X..", "XXX"], 0.6), ("plus", [".X.", "XXX", ".X."], 0.4),
    ("L5", ["X.", "X.", "X.", "XX"], 0.6), ("u5", ["X.X", "XXX"], 0.45), ("stair6", ["X..", "XX.", "XXX"], 0.35),
    ("w5", ["X..", "XX.", ".XX"], 0.4), ("t5", ["XXX", ".X.", ".X."], 0.4), ("z5", ["XX.", ".X.", ".XX"], 0.4),
    ("p5", ["XX", "XX", "X."], 0.5), ("n5", [".X", "XX", "X.", "X."], 0.3), ("y5", [".X", "XX", ".X", ".X"], 0.3),
    ("rect2x4", ["XXXX", "XXXX"], 0.2),
]


def _normalize(cells):
    min_x = min(c[0] for c in cells)
    min_y = min(c[1] for c in cells)
    return sorted(((x - min_x, y - min_y) for x, y in cells), key=lambda c: (c[1], c[0]))


def _rotate(cells):
    return _normalize([(-y, x) for x, y in cells])


def build_shapes():
    shapes = []
    for name, rows, weight in BASES:
        cells = [(x, y) for y, row in enumerate(rows) for x, ch in enumerate(row) if ch == "X"]
        current = _normalize(cells)
        seen, orientations = set(), []
        for _ in range(4):
            key = tuple(current)
            if key not in seen:
                seen.add(key)
                orientations.append(current)
            current = _rotate(current)
        for orientation in orientations:
            shapes.append({"name": name, "cells": tuple(orientation), "weight": weight / len(orientations)})
    return shapes


SHAPES = build_shapes()
# Multiplayer uses only pieces of at most 5 blocks, so nobody gets locked out by a huge piece.
MP_POOL = [s for s in SHAPES if len(s["cells"]) <= 5]


def generate_sets(seed, count=SET_COUNT):
    """The shared piece sequence of a match: a list of sets; each set = 3 pieces (name, color, cells)."""
    rng = random.Random(seed)
    weights = [s["weight"] for s in MP_POOL]
    sets = []
    for _ in range(count):
        picks = rng.choices(MP_POOL, weights=weights, k=SET_SIZE)
        if all(len(p["cells"]) >= 5 for p in picks):            # always at least one small piece per set
            small = [s for s in MP_POOL if len(s["cells"]) <= 4]
            picks[-1] = rng.choice(small)
        sets.append([{"n": p["name"], "k": rng.randrange(4), "c": [list(c) for c in p["cells"]]} for p in picks])
    return sets


def clear_points(lines, combo):
    """50 * lines^2 * multiplier, in integers (multiplier * 2 = combo + 1, capped at 10)."""
    if lines <= 0:
        return 0
    return 25 * lines * lines * min(combo + 1, 10)


class MoveError(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class PlayerState:
    """Authoritative state of one player during a match."""

    def __init__(self, sets):
        self.sets = sets
        self.board = [-1] * (GRID * GRID)
        self.set_index = 0
        self.used = [False] * SET_SIZE
        self.score = 0
        self.combo = 0
        self.max_combo = 0
        self.moves = 0
        self.lines = 0
        self.perfect_clears = 0
        self.finished = False
        self.finish_reason = ""
        self.violations = 0
        self.last_move_at = -1e9
        self._check_stuck()

    # ---- queries
    def current_set(self):
        return self.sets[self.set_index] if self.set_index < len(self.sets) else None

    def can_place(self, cells, x, y):
        for cx, cy in cells:
            px, py = x + cx, y + cy
            if not (0 <= px < GRID and 0 <= py < GRID) or self.board[py * GRID + px] != -1:
                return False
        return True

    def has_move(self):
        current = self.current_set()
        if current is None:
            return False
        for slot, piece in enumerate(current):
            if self.used[slot]:
                continue
            cells = piece["c"]
            for y in range(GRID):
                for x in range(GRID):
                    if self.can_place(cells, x, y):
                        return True
        return False

    def snapshot(self):
        return {"score": self.score, "combo": self.combo, "moves": self.moves, "set_index": self.set_index,
                "used": list(self.used), "board": list(self.board), "finished": self.finished}

    # ---- moves
    def apply_move(self, slot, x, y):
        if self.finished:
            raise MoveError("finished")
        current = self.current_set()
        if current is None or not (0 <= slot < SET_SIZE):
            raise MoveError("bad_slot")
        if self.used[slot]:
            raise MoveError("used")
        piece = current[slot]
        cells = piece["c"]
        if not self.can_place(cells, x, y):
            raise MoveError("illegal")

        for cx, cy in cells:
            self.board[(y + cy) * GRID + (x + cx)] = piece["k"]
        self.used[slot] = True
        self.moves += 1
        gained = len(cells)

        full_rows = [r for r in range(GRID) if all(self.board[r * GRID + c] != -1 for c in range(GRID))]
        full_cols = [c for c in range(GRID) if all(self.board[r * GRID + c] != -1 for r in range(GRID))]
        lines = len(full_rows) + len(full_cols)
        perfect = False
        if lines:
            self.combo += 1
            self.max_combo = max(self.max_combo, self.combo)
            to_clear = {(c, r) for r in full_rows for c in range(GRID)} | {(c, r) for c in full_cols for r in range(GRID)}
            for c, r in to_clear:
                self.board[r * GRID + c] = -1
            gained += clear_points(lines, self.combo)
            if all(v == -1 for v in self.board):
                gained += PERFECT_CLEAR_BONUS
                perfect = True
                self.perfect_clears += 1
        else:
            self.combo = 0
        self.score += gained
        self.lines += lines

        if all(self.used):
            self.set_index += 1
            self.used = [False] * SET_SIZE
        self._check_stuck()
        return {"gained": gained, "lines": lines, "perfect": perfect}

    def _check_stuck(self):
        if not self.finished and not self.has_move():
            self.finished = True
            self.finish_reason = "stuck"
