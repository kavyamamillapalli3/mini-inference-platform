"""
Gateway: the front door. Decides WHETHER a request may enter.

  auth (API key -> tenant) -> size caps by tier -> concurrency limit
  -> token bucket (tokens/min) -> proxy to replica, streaming
  -> on finish, refund unused tokens

Errors are explicit and cheap:
  401 unknown key | 400 too large for this tier
  429 over your limit (with Retry-After) | 529 replica overloaded

Run:
  UPSTREAM=http://localhost:8001 python -m uvicorn gateway.server:app --port 8000
"""

import json
import math
import os
import time

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Route

from gateway import upstream
from gateway.limits import Tenant

UPSTREAM = os.environ.get("UPSTREAM", "http://localhost:8001")
TENANTS_FILE = os.environ.get(
    "TENANTS_FILE", os.path.join(os.path.dirname(__file__), "tenants.json"))


def load_tenants():
    with open(TENANTS_FILE) as f:
        cfg = json.load(f)
    return {key: Tenant(name=c["tenant"], tier=c["tier"],
                        tokens_per_min=c["tokens_per_min"],
                        max_concurrency=c["max_concurrency"],
                        max_prompt_tokens=c["max_prompt_tokens"],
                        max_output_tokens=c["max_output_tokens"])
            for key, c in cfg.items()}


TENANTS = load_tenants()


def estimate_prompt_tokens(body: dict) -> int:
    text = []
    for m in body.get("messages", []):
        c = m.get("content", "")
        if isinstance(c, list):
            c = " ".join(p.get("text", "") for p in c if isinstance(p, dict))
        text.append(str(c))
    return max(1, len("\n".join(text)) // 4)   # ~4 chars per token


def error(status, kind, message, retry_after=None):
    headers = {"Retry-After": str(max(1, math.ceil(retry_after)))} if retry_after else {}
    return JSONResponse({"error": {"type": kind, "message": message}},
                        status_code=status, headers=headers)


async def chat_completions(request: Request):
    # 1. Auth
    key = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
    tenant = TENANTS.get(key)
    if tenant is None:
        return error(401, "authentication_error", "unknown API key")

    body = await request.json()
    prompt_tokens = estimate_prompt_tokens(body)
    max_tokens = int(body.get("max_tokens") or 256)

    # 2. Size caps by tier (protects KV memory: "no KV-cache DoS")
    if prompt_tokens > tenant.max_prompt_tokens or max_tokens > tenant.max_output_tokens:
        tenant.stats["rejected_too_large"] += 1
        return error(400, "invalid_request_error",
                     f"{tenant.tier} tier allows {tenant.max_prompt_tokens} prompt tokens "
                     f"and {tenant.max_output_tokens} output tokens")

    # 3. Concurrency limit
    if tenant.in_flight >= tenant.max_concurrency:
        tenant.stats["rejected_concurrency"] += 1
        return error(429, "rate_limit_error",
                     f"too many concurrent requests (limit {tenant.max_concurrency})",
                     retry_after=1)

    # 4. Token bucket: charge the worst case now, refund the unused part later
    estimate = prompt_tokens + max_tokens
    ok, retry_after = tenant.bucket.try_consume(estimate)
    if not ok:
        if retry_after is None:
            tenant.stats["rejected_too_large"] += 1
            return error(400, "invalid_request_error", "request exceeds per-minute budget")
        tenant.stats["rejected_rate"] += 1
        return error(429, "rate_limit_error", "token rate limit exceeded",
                     retry_after=retry_after)

    tenant.in_flight += 1
    tenant.stats["allowed"] += 1
    tenant.stats["tokens_charged"] += estimate
    used = {"completion": 0}

    def finish():
        tenant.in_flight -= 1
        actual = prompt_tokens + used["completion"]
        refund = max(0, estimate - actual)
        tenant.bucket.refund(refund)
        tenant.stats["tokens_refunded"] += refund

    # 5. Proxy to the replica
    if body.get("stream"):
        body["stream_options"] = {"include_usage": True}   # so we can meter actual tokens
    try:
        up = await upstream.post(f"{UPSTREAM}/v1/chat/completions", json.dumps(body).encode(),
                                 headers={"X-Tier": tenant.tier, "X-Tenant": tenant.name})
    except (OSError, TimeoutError) as e:
        finish()
        return error(503, "unavailable", f"no replica reachable: {e}")

    if up.status != 200:
        await up.read()
        up.close()
        finish()
        if up.status in (503, 529):
            tenant.stats["upstream_overloaded"] += 1
            return error(529, "overloaded_error", "replicas are overloaded", retry_after=1)
        return error(502, "upstream_error", f"replica returned {up.status}")

    if not body.get("stream"):
        raw = await up.read()
        up.close()
        data = json.loads(raw)
        used["completion"] = data.get("usage", {}).get("completion_tokens", 0)
        finish()
        return JSONResponse(data)

    async def relay():
        buf = b""
        try:
            async for piece in up.iter_body():
                yield piece
                buf += piece
                while b"\n" in buf:               # peek at SSE lines to read usage
                    line, buf = buf.split(b"\n", 1)
                    if line.startswith(b"data:") and b'"usage"' in line:
                        try:
                            u = json.loads(line[5:]).get("usage") or {}
                            used["completion"] = u.get("completion_tokens", used["completion"])
                        except ValueError:
                            pass
        finally:
            # Runs on normal end AND on client disconnect: closing the upstream
            # connection lets the replica cancel generation and free KV memory.
            up.close()
            finish()

    return StreamingResponse(relay(), media_type="text/event-stream")


async def metrics(_):
    lines = []
    for t in TENANTS.values():
        lab = f'{{tenant="{t.name}",tier="{t.tier}"}}'
        lines.append(f"gateway_in_flight{lab} {t.in_flight}")
        lines.append(f"gateway_bucket_tokens{lab} {int(t.bucket.tokens)}")
        for k, v in t.stats.items():
            lines.append(f"gateway_{k}_total{lab} {v}")
    return PlainTextResponse("\n".join(lines) + "\n")


async def health(_):
    return PlainTextResponse("ok")


app = Starlette(routes=[
    Route("/v1/chat/completions", chat_completions, methods=["POST"]),
    Route("/metrics", metrics),
    Route("/health", health),
])
