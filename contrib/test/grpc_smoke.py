#!/usr/bin/env python3
"""Dependency-free gRPC client for exercising grpc_server_live.

Speaks just enough HTTP/2 by hand to drive one server-streaming RPC,
using only the Python standard library, for hosts without grpcio.

    contrib/test/grpc_smoke.py                            # one RPC
    contrib/test/grpc_smoke.py --slow 0.5                 # slow reader, forces eviction
    contrib/test/grpc_smoke.py --path /test.v1.Echo/Nope  # expect UNIMPLEMENTED

Not a general HTTP/2 implementation: request headers are emitted as
static-indexed or literal entries only (no HPACK dynamic table), and the
response trailer scan assumes the framing fd_grpc_server emits.
"""

import argparse
import socket
import sys
import time

PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

DATA, HEADERS, RST_STREAM, SETTINGS, PING, GOAWAY, WINDOW_UPDATE = 0, 1, 3, 4, 6, 7, 8
END_STREAM, END_HEADERS, ACK = 0x1, 0x4, 0x1

GRPC_STATUS = {
    0: "OK", 1: "CANCELLED", 2: "UNKNOWN", 3: "INVALID_ARGUMENT",
    4: "DEADLINE_EXCEEDED", 5: "NOT_FOUND", 6: "ALREADY_EXISTS",
    7: "PERMISSION_DENIED", 8: "RESOURCE_EXHAUSTED", 9: "FAILED_PRECONDITION",
    10: "ABORTED", 11: "OUT_OF_RANGE", 12: "UNIMPLEMENTED", 13: "INTERNAL",
    14: "UNAVAILABLE", 15: "DATA_LOSS", 16: "UNAUTHENTICATED",
}


def frame(ftype, flags, sid, payload=b""):
    return len(payload).to_bytes(3, "big") + bytes([ftype, flags]) + sid.to_bytes(4, "big") + payload


def hpack_indexed(idx):
    return bytes([0x80 | idx])


def hpack_literal_indexed_name(idx, value):
    v = value.encode()
    assert len(v) < 127
    prefix = bytes([idx]) if idx < 15 else bytes([0x0F, idx - 15])
    return prefix + bytes([len(v)]) + v


def hpack_literal_new_name(name, value):
    n, v = name.encode(), value.encode()
    assert len(n) < 127 and len(v) < 127
    return b"\x00" + bytes([len(n)]) + n + bytes([len(v)]) + v


class Conn:
    def __init__(self, host, port, timeout):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.buf = b""
        self.sock.sendall(PREFACE + frame(SETTINGS, 0, 0))

    def _fill(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("peer closed connection")
            self.buf += chunk

    def read_frame(self):
        self._fill(9)
        length = int.from_bytes(self.buf[0:3], "big")
        ftype, flags = self.buf[3], self.buf[4]
        sid = int.from_bytes(self.buf[5:9], "big") & 0x7FFFFFFF
        self._fill(9 + length)
        payload = self.buf[9 : 9 + length]
        self.buf = self.buf[9 + length :]
        return ftype, flags, sid, payload

    def send(self, *args, **kwargs):
        self.sock.sendall(frame(*args, **kwargs))


def parse_grpc_status(payload):
    """Best-effort scan of a trailer block for grpc-status."""
    i = payload.find(b"grpc-status")
    if i < 0:
        return None
    j = i + len(b"grpc-status")
    if j >= len(payload):
        return None
    vlen = payload[j]
    try:
        return int(payload[j + 1 : j + 1 + vlen])
    except ValueError:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--path", default="/test.v1.Echo/Stream")
    ap.add_argument("--body", default="hi")
    ap.add_argument("--slow", type=float, default=0.0, help="sleep per message, forces eviction")
    ap.add_argument("--max", type=int, default=0, help="stop after N messages (0 = until server closes)")
    ap.add_argument("--timeout", type=float, default=10.0)
    args = ap.parse_args()

    conn = Conn(args.host, args.port, args.timeout)

    hdrs = (
        hpack_indexed(3)                                                  # :method: POST
        + hpack_indexed(6)                                                # :scheme: http
        + hpack_literal_indexed_name(4, args.path)                        # :path
        + hpack_literal_indexed_name(1, f"{args.host}:{args.port}")       # :authority
        + hpack_literal_indexed_name(31, "application/grpc+proto")        # content-type
        + hpack_literal_new_name("te", "trailers")
    )
    conn.send(HEADERS, END_HEADERS, 1, hdrs)

    body = args.body.encode()
    conn.send(DATA, END_STREAM, 1, b"\x00" + len(body).to_bytes(4, "big") + body)

    pending = b""
    messages = []
    status = None
    consumed = 0
    t0 = time.time()

    while True:
        ftype, flags, sid, payload = conn.read_frame()

        if ftype == SETTINGS and not (flags & ACK):
            conn.send(SETTINGS, ACK, 0)
        elif ftype == PING and not (flags & ACK):
            conn.send(PING, ACK, 0, payload)
        elif ftype == GOAWAY:
            code = int.from_bytes(payload[4:8], "big") if len(payload) >= 8 else -1
            print(f"GOAWAY error={code}", file=sys.stderr)
            break
        elif ftype == RST_STREAM:
            code = int.from_bytes(payload[0:4], "big") if len(payload) >= 4 else -1
            print(f"RST_STREAM error={code} (server dropped us)", file=sys.stderr)
            break
        elif ftype == DATA:
            pending += payload
            consumed += len(payload)
            # Refill both windows so a long stream doesn't stall at 64KB.
            if consumed > 32768:
                conn.send(WINDOW_UPDATE, 0, 0, consumed.to_bytes(4, "big"))
                conn.send(WINDOW_UPDATE, 0, 1, consumed.to_bytes(4, "big"))
                consumed = 0
            while len(pending) >= 5:
                msg_len = int.from_bytes(pending[1:5], "big")
                if len(pending) < 5 + msg_len:
                    break
                messages.append(pending[5 : 5 + msg_len])
                pending = pending[5 + msg_len :]
                if args.slow:
                    time.sleep(args.slow)
                if args.max and len(messages) >= args.max:
                    break
            if args.max and len(messages) >= args.max:
                break
            if flags & END_STREAM:
                break
        elif ftype == HEADERS:
            got = parse_grpc_status(payload)
            if got is not None:
                status = got
            if flags & END_STREAM:
                break

    dt = time.time() - t0
    print(f"messages: {len(messages)} in {dt:.2f}s")
    for m in messages[:8]:
        print(f"  {m[:48]!r}")
    if len(messages) > 8:
        print(f"  ... {len(messages)-8} more")
    if status is not None:
        print(f"grpc-status: {status} ({GRPC_STATUS.get(status, '?')})")
    else:
        print("grpc-status: <none received>")
    return 0


if __name__ == "__main__":
    sys.exit(main())