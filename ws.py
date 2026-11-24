"""Minimal WebSocket (RFC 6455) server side, standard library only.

Handles: HTTP upgrade handshake, text frames, fragmentation, ping/pong, close frames,
client-to-server masking, message size limits. Plain HTTP requests (health checks) are
answered by the caller.
"""
import asyncio
import base64
import hashlib
import struct

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_HEADER_BYTES = 16384


class ProtocolError(Exception):
    def __init__(self, code=1002, reason="protocol error"):
        super().__init__(reason)
        self.code = code
        self.reason = reason


async def read_http_request(reader, timeout=10.0):
    """Returns (method, path, headers) or None if the request is unusable."""
    try:
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout)
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError):
        return None
    if len(raw) > MAX_HEADER_BYTES:
        return None
    try:
        text = raw.decode("latin-1")
    except UnicodeDecodeError:
        return None
    lines = text.split("\r\n")
    parts = lines[0].split(" ")
    if len(parts) < 3:
        return None
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()
    return parts[0], parts[1], headers


def http_response(status, body, content_type="text/plain; charset=utf-8", extra=None):
    reasons = {200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 426: "Upgrade Required",
               503: "Service Unavailable"}
    data = body.encode("utf-8") if isinstance(body, str) else body
    head = f"HTTP/1.1 {status} {reasons.get(status, 'Status')}\r\nContent-Type: {content_type}\r\n" \
           f"Content-Length: {len(data)}\r\nConnection: close\r\nCache-Control: no-store\r\n"
    for key, value in (extra or {}).items():
        head += f"{key}: {value}\r\n"
    return head.encode("latin-1") + b"\r\n" + data


def is_upgrade(headers):
    return headers.get("upgrade", "").lower() == "websocket" and "upgrade" in headers.get("connection", "").lower()


def accept_key(client_key):
    return base64.b64encode(hashlib.sha1((client_key + GUID).encode("ascii")).digest()).decode("ascii")


def handshake_response(headers):
    """Returns the 101 response bytes, or None if the handshake request is invalid."""
    key = headers.get("sec-websocket-key", "")
    if headers.get("sec-websocket-version") != "13":
        return None
    try:
        if len(base64.b64decode(key, validate=True)) != 16:
            return None
    except Exception:
        return None
    return ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept_key(key)}\r\n\r\n").encode("latin-1")


def _unmask(data, mask):
    n = len(data)
    if n == 0:
        return b""
    full = (mask * (n // 4 + 1))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(full, "big")).to_bytes(n, "big")


class WebSocket:
    def __init__(self, reader, writer, max_message=65536):
        self.reader = reader
        self.writer = writer
        self.max_message = max_message
        self.closed = False
        self._close_sent = False
        self._lock = asyncio.Lock()

    async def _read_frame(self):
        head = await self.reader.readexactly(2)
        b1, b2 = head[0], head[1]
        fin = bool(b1 & 0x80)
        if b1 & 0x70:
            raise ProtocolError(1002, "reserved bits set")
        opcode = b1 & 0x0F
        masked = bool(b2 & 0x80)
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", await self.reader.readexactly(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", await self.reader.readexactly(8))[0]
        if not masked:
            raise ProtocolError(1002, "client frames must be masked")
        if opcode >= 0x8 and (length > 125 or not fin):
            raise ProtocolError(1002, "bad control frame")
        if length > self.max_message:
            raise ProtocolError(1009, "message too big")
        mask = await self.reader.readexactly(4)
        payload = _unmask(await self.reader.readexactly(length), mask) if length else b""
        return fin, opcode, payload

    async def recv(self):
        """Next text message, or None when the connection is closed."""
        buffer = bytearray()
        started = False
        try:
            while True:
                fin, opcode, payload = await self._read_frame()
                if opcode == 0x8:                      # close
                    code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1000
                    await self._send_close(code if code not in (1005, 1006) else 1000)
                    return None
                if opcode == 0x9:                      # ping
                    await self._send_frame(0xA, payload)
                    continue
                if opcode == 0xA:                      # pong
                    continue
                if opcode in (0x1, 0x2):
                    if started:
                        raise ProtocolError(1002, "new message inside fragmented message")
                    if opcode == 0x2:
                        raise ProtocolError(1003, "binary frames are not supported")
                    started = True
                elif opcode == 0x0:
                    if not started:
                        raise ProtocolError(1002, "unexpected continuation")
                else:
                    raise ProtocolError(1002, "unknown opcode")
                buffer += payload
                if len(buffer) > self.max_message:
                    raise ProtocolError(1009, "message too big")
                if fin:
                    try:
                        return buffer.decode("utf-8")
                    except UnicodeDecodeError:
                        raise ProtocolError(1007, "invalid utf-8")
        except ProtocolError as err:
            await self._send_close(err.code, err.reason)
            return None
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            return None

    async def _send_frame(self, opcode, payload=b""):
        if self.closed:
            return
        length = len(payload)
        if length < 126:
            head = struct.pack("!BB", 0x80 | opcode, length)
        elif length < 65536:
            head = struct.pack("!BBH", 0x80 | opcode, 126, length)
        else:
            head = struct.pack("!BBQ", 0x80 | opcode, 127, length)
        async with self._lock:
            try:
                self.writer.write(head + payload)
                await self.writer.drain()
            except (ConnectionError, OSError):
                self.closed = True

    async def send_text(self, text):
        await self._send_frame(0x1, text.encode("utf-8"))

    async def _send_close(self, code=1000, reason=""):
        if self._close_sent:
            return
        self._close_sent = True
        await self._send_frame(0x8, struct.pack("!H", code) + reason.encode("utf-8")[:100])

    async def close(self, code=1000, reason=""):
        await self._send_close(code, reason)
        self.closed = True
        try:
            self.writer.close()
        except Exception:
            pass
