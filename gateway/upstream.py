"""
Tiny async HTTP/1.1 client for talking to a replica, with streaming.

Kept dependency-free on purpose so you can see what a proxy does:
open a connection, send the request, read the status and headers, then pass
the body through chunk by chunk. Closing the connection early tells the
replica the client is gone (so it can cancel generation and free KV memory).
"""

import asyncio
from urllib.parse import urlparse


class UpstreamResponse:
    def __init__(self, status, headers, reader, writer):
        self.status = status
        self.headers = headers
        self._reader = reader
        self._writer = writer

    async def iter_body(self):
        r = self._reader
        if self.headers.get("transfer-encoding", "").lower() == "chunked":
            while True:
                size_line = await r.readline()
                if not size_line:
                    return
                size = int(size_line.strip().split(b";")[0], 16)
                if size == 0:
                    await r.readline()  # trailing CRLF
                    return
                data = await r.readexactly(size)
                await r.readexactly(2)  # CRLF after each chunk
                yield data
        elif "content-length" in self.headers:
            n = int(self.headers["content-length"])
            if n:
                yield await r.readexactly(n)
        else:
            while True:
                data = await r.read(65536)
                if not data:
                    return
                yield data

    async def read(self) -> bytes:
        return b"".join([c async for c in self.iter_body()])

    def close(self):
        try:
            self._writer.close()
        except Exception:
            pass


async def post(url: str, body: bytes, timeout: float = 10.0) -> UpstreamResponse:
    u = urlparse(url)
    host, port = u.hostname, u.port or 80
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    head = (f"POST {u.path or '/'} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n").encode()
    writer.write(head + body)
    await writer.drain()

    status_line = await asyncio.wait_for(reader.readline(), timeout)
    status = int(status_line.split()[1])
    headers = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        k, _, v = line.decode().partition(":")
        headers[k.strip().lower()] = v.strip()
    return UpstreamResponse(status, headers, reader, writer)
