"""A small independent WebSocket CLIENT used by the tests (written separately from server/ws.py)."""
import asyncio
import base64
import json
import os
import ssl
import struct
import time


class Client:
    def __init__(self, label="client"):
        self.label = label
        self.messages = []
        self.closed = False
        self.close_code = None
        self._reader = None
        self._writer = None
        self._task = None

    async def connect(self, host, port, use_tls=False, origin=None, path="/"):
        ctx = None
        if use_tls:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        self._reader, self._writer = await asyncio.open_connection(host, port, ssl=ctx)
        key = base64.b64encode(os.urandom(16)).decode()
        lines = [f"GET {path} HTTP/1.1", f"Host: {host}:{port}", "Upgrade: websocket", "Connection: Upgrade",
                 f"Sec-WebSocket-Key: {key}", "Sec-WebSocket-Version: 13"]
        if origin:
            lines.append(f"Origin: {origin}")
        self._writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await self._writer.drain()
        head = await self._reader.readuntil(b"\r\n\r\n")
        status = head.split(b"\r\n")[0].decode()
        if " 101 " not in status:
            self.closed = True
            raise ConnectionError(status)
        self._task = asyncio.ensure_future(self._read_loop())
        return self

    async def _read_loop(self):
        try:
            while True:
                head = await self._reader.readexactly(2)
                fin, opcode = bool(head[0] & 0x80), head[0] & 0x0F
                length = head[1] & 0x7F
                if length == 126:
                    length = struct.unpack("!H", await self._reader.readexactly(2))[0]
                elif length == 127:
                    length = struct.unpack("!Q", await self._reader.readexactly(8))[0]
                payload = await self._reader.readexactly(length)
                if opcode == 0x1:
                    self.messages.append(json.loads(payload.decode()))
                elif opcode == 0x9:
                    await self._send_frame(0xA, payload)
                elif opcode == 0x8:
                    self.close_code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1005
                    break
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        self.closed = True

    async def _send_frame(self, opcode, payload):
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        n = len(payload)
        head = bytes([0x80 | opcode])
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack("!H", n)
        else:
            head += bytes([0x80 | 127]) + struct.pack("!Q", n)
        self._writer.write(head + mask + masked)
        await self._writer.drain()

    async def send(self, message):
        await self._send_frame(0x1, json.dumps(message).encode())

    async def send_raw_text(self, text):
        await self._send_frame(0x1, text.encode())

    async def wait(self, msg_type, timeout=5.0, where=None):
        """Waits for (and removes) the first message of this type. Raises TimeoutError."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for i, m in enumerate(self.messages):
                if m.get("t") == msg_type and (where is None or where(m)):
                    return self.messages.pop(i)
            if self.closed and not any(m.get("t") == msg_type for m in self.messages):
                break
            await asyncio.sleep(0.01)
        raise TimeoutError(f"{self.label}: no '{msg_type}' within {timeout}s; got {[m.get('t') for m in self.messages]}")

    def drain(self, msg_type):
        found = [m for m in self.messages if m.get("t") == msg_type]
        self.messages = [m for m in self.messages if m.get("t") != msg_type]
        return found

    async def abort(self):
        """Simulates the network dying: the TCP connection is dropped without a close frame."""
        self.closed = True
        try:
            self._writer.transport.abort()
        except Exception:
            pass

    async def close(self):
        if not self.closed:
            try:
                await self._send_frame(0x8, struct.pack("!H", 1000))
                await asyncio.sleep(0.05)
            except Exception:
                pass
        try:
            self._writer.close()
        except Exception:
            pass
        if self._task:
            self._task.cancel()
