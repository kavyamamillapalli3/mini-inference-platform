"""
Router: decides WHERE and WHEN a request runs.

  tier queues (in memory, bounded, with deadlines)
  -> weighted priority (paid first, free not starved)
  -> pick a replica: healthy, has a free slot, ideally the one that already
     caches this request's system prompt (affinity), else least loaded
  -> proxy the stream; release the slot when done

Failure rules:
  * replica unreachable before any token -> mark it down, retry once elsewhere
  * replica dies mid-stream              -> send an error event, never retry silently
  * deadline passes while queued         -> 529 (fail fast, don't serve late)

Run:
  REPLICAS=http://localhost:8011,http://localhost:8012 \
    python -m uvicorn router.server:app --port 8002

Env:
  POLICY=priority|fifo        ROUTING=affinity|least_loaded|random
  SLOTS_PER_REPLICA=48        (below the replica's batch size = headroom)
  DEADLINE_MS_PAID=3000  DEADLINE_MS_FREE=1000  MAX_QUEUE=200
  FREE_SLOT_SHARE=0.6         (Free can use at most 60% of slots; priority mode only)
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import random
import time
from collections import deque

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Route

from gateway import upstream

REPLICA_URLS = [u.strip() for u in os.environ.get(
    "REPLICAS", "http://localhost:8011,http://localhost:8012").split(",") if u.strip()]
POLICY = os.environ.get("POLICY", "priority")
ROUTING = os.environ.get("ROUTING", "affinity")
SLOTS = int(os.environ.get("SLOTS_PER_REPLICA", 48))
DEADLINE_MS = {"paid": float(os.environ.get("DEADLINE_MS_PAID", 3000)),
               "free": float(os.environ.get("DEADLINE_MS_FREE", 1000))}
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", 200))
WEIGHTS = {"paid": 4, "free": 1}          # 4 paid dispatches per 1 free when both wait
# Reserved capacity: Free may hold at most this share of all slots, so Paid
# always finds room (priority alone can't help once Free fills every slot).
FREE_SLOT_SHARE = float(os.environ.get("FREE_SLOT_SHARE", 0.6))
HEALTH_EVERY_S = 0.5
FAILS_TO_EJECT = 3


class Replica:
    def __init__(self, url):
        self.url = url
        self.in_flight = 0
        self.healthy = True
        self.fails = 0
        self.dispatched = 0

    def has_room(self):
        return self.healthy and self.in_flight < SLOTS


class Pending:
    def __init__(self, tier, affinity_key):
        self.tier = tier
        self.affinity_key = affinity_key
        self.enqueued = time.monotonic()
        self.deadline = self.enqueued + DEADLINE_MS.get(tier, 1000) / 1000
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()


class Router:
    def __init__(self):
        self.replicas = [Replica(u) for u in REPLICA_URLS]
        self.queues = {"paid": deque(), "free": deque()} if POLICY == "priority" \
            else {"all": deque()}
        self._wrr = [t for t, w in WEIGHTS.items() for _ in range(w)]  # paid x4, free x1
        self._wrr_i = 0
        self.tier_in_flight = {"paid": 0, "free": 0}
        self.stats = {"dispatched": 0, "affinity_hits": 0, "affinity_spills": 0,
                      "expired": 0, "rejected_queue_full": 0, "retries": 0,
                      "midstream_failures": 0}

    # ---------- choosing a replica ----------
    def _preferred(self, key):
        """Rendezvous hashing: each key maps to a stable replica, and only keys
        on a removed replica move when the set changes."""
        healthy = [r for r in self.replicas if r.healthy]
        if not healthy or not key:
            return None
        return max(healthy, key=lambda r: hashlib.md5(f"{key}|{r.url}".encode()).digest())

    def _pick_replica(self, key):
        open_ = [r for r in self.replicas if r.has_room()]
        if not open_:
            return None
        if ROUTING == "random":
            return random.choice(open_)
        if ROUTING == "affinity":
            pref = self._preferred(key)
            if pref is not None and pref.has_room():
                self.stats["affinity_hits"] += 1
                return pref
            self.stats["affinity_spills"] += 1
        return min(open_, key=lambda r: r.in_flight)

    # ---------- queueing ----------
    def _queue_for(self, tier):
        return self.queues[tier] if POLICY == "priority" else self.queues["all"]

    def _free_capped(self):
        total = SLOTS * sum(1 for r in self.replicas if r.healthy)
        return self.tier_in_flight["free"] >= FREE_SLOT_SHARE * total

    def _next_pending(self):
        if POLICY != "priority":
            q = self.queues["all"]
            return q.popleft() if q else None
        for _ in range(len(self._wrr)):            # weighted round robin across tiers
            tier = self._wrr[self._wrr_i]
            self._wrr_i = (self._wrr_i + 1) % len(self._wrr)
            if tier == "free" and self._free_capped():
                continue                           # leave Free waiting; slots stay for Paid
            if self.queues[tier]:
                return self.queues[tier].popleft()
        return None

    def pump(self):
        """Hand queued requests to replicas while there are free slots."""
        while True:
            if not any(r.has_room() for r in self.replicas):
                return
            p = self._next_pending()
            if p is None:
                return
            if p.future.done():                      # already timed out or cancelled
                continue
            r = self._pick_replica(p.affinity_key)
            r.in_flight += 1
            self.tier_in_flight[p.tier] += 1
            r.dispatched += 1
            self.stats["dispatched"] += 1
            p.future.set_result(r)

    async def acquire(self, tier, key, deadline=None):
        q = self._queue_for(tier)
        if len(q) >= MAX_QUEUE:
            self.stats["rejected_queue_full"] += 1
            self.stats[f"rejected_queue_full_{tier}"] = self.stats.get(f"rejected_queue_full_{tier}", 0) + 1
            return None, "queue_full"
        p = Pending(tier, key)
        if deadline:
            p.deadline = deadline
        q.append(p)
        self.pump()
        try:
            r = await asyncio.wait_for(p.future, max(0.0, p.deadline - time.monotonic()))
            return r, p.deadline
        except asyncio.TimeoutError:
            self.stats["expired"] += 1
            self.stats[f"expired_{tier}"] = self.stats.get(f"expired_{tier}", 0) + 1
            return None, "deadline"

    def release(self, r, tier):
        r.in_flight -= 1
        self.tier_in_flight[tier] -= 1
        self.pump()

    def mark_failed(self, r, hard=False):
        r.fails += 1
        if hard or r.fails >= FAILS_TO_EJECT:
            r.healthy = False

    # ---------- health checks ----------
    async def health_loop(self):
        while True:
            for r in self.replicas:
                try:
                    resp = await upstream.get(f"{r.url}/health", timeout=1.0)
                    await resp.read()
                    resp.close()
                    ok = resp.status == 200
                except (OSError, asyncio.TimeoutError, ValueError, IndexError):
                    ok = False
                if ok:
                    r.fails = 0
                    if not r.healthy:
                        r.healthy = True
                        self.pump()
                else:
                    self.mark_failed(r)
            await asyncio.sleep(HEALTH_EVERY_S)


router = Router()


def affinity_key(body):
    """Requests with the same system prompt share a key, so they go to the
    replica that already has that prefix in its KV cache."""
    sys_text = "\n".join(str(m.get("content", "")) for m in body.get("messages", [])
                         if m.get("role") == "system")
    return hashlib.sha1(sys_text.encode()).hexdigest() if sys_text else None


def overloaded(reason):
    return JSONResponse({"error": {"type": "overloaded_error", "message": reason}},
                        status_code=529, headers={"Retry-After": "1"})


async def chat_completions(request: Request):
    raw = await request.body()
    body = json.loads(raw)
    tier = request.headers.get("x-tier", "free").lower()
    if tier not in ("paid", "free"):
        tier = "free"
    key = affinity_key(body)

    deadline = None
    for attempt in range(2):                       # at most one retry, only before streaming
        r, info = await router.acquire(tier, key, deadline)
        if r is None:
            return overloaded("queue full" if info == "queue_full"
                              else "no capacity before the deadline")
        deadline = info
        try:
            up = await upstream.post(f"{r.url}/v1/chat/completions", raw, timeout=5.0)
        except (OSError, asyncio.TimeoutError):
            router.mark_failed(r, hard=True)         # connection refused: eject now
            router.release(r, tier)
            router.stats["retries"] += 1
            continue
        if up.status == 503:                         # replica full: try another one
            await up.read()
            up.close()
            router.release(r, tier)
            router.stats["retries"] += 1
            continue
        break
    else:
        router.stats["unavailable"] = router.stats.get("unavailable", 0) + 1
        return overloaded("replicas unavailable")

    if up.status != 200:
        data = await up.read()
        up.close()
        router.release(r, tier)
        return JSONResponse(json.loads(data or b"{}"), status_code=up.status)

    if not body.get("stream"):
        data = await up.read()
        up.close()
        router.release(r, tier)
        return JSONResponse(json.loads(data))

    async def relay():
        try:
            async for piece in up.iter_body():
                yield piece
        except (OSError, asyncio.IncompleteReadError, ValueError):
            # Replica died after tokens were sent: tell the client, don't retry
            # silently (a new attempt would produce a different answer).
            router.stats["midstream_failures"] += 1
            router.mark_failed(r)
            err = {"error": {"type": "replica_failed",
                             "message": "generation interrupted, please retry"}}
            yield f"data: {json.dumps(err)}\n\n".encode()
            yield b"data: [DONE]\n\n"
        finally:
            up.close()
            router.release(r, tier)

    return StreamingResponse(relay(), media_type="text/event-stream")


async def metrics(_):
    lines = [f'router_queue_depth{{queue="{k}"}} {len(q)}' for k, q in router.queues.items()]
    lines += [f'router_tier_in_flight{{tier="{k}"}} {v}' for k, v in router.tier_in_flight.items()]
    for r in router.replicas:
        lab = f'{{replica="{r.url}"}}'
        lines += [f"router_replica_in_flight{lab} {r.in_flight}",
                  f"router_replica_healthy{lab} {int(r.healthy)}",
                  f"router_replica_dispatched_total{lab} {r.dispatched}"]
    lines += [f"router_{k}_total {v}" for k, v in router.stats.items()]
    return PlainTextResponse("\n".join(lines) + "\n")


async def health(_):
    return PlainTextResponse("ok")


@contextlib.asynccontextmanager
async def lifespan(_app):
    task = asyncio.get_running_loop().create_task(router.health_loop())
    yield
    task.cancel()


app = Starlette(routes=[
    Route("/v1/chat/completions", chat_completions, methods=["POST"]),
    Route("/metrics", metrics),
    Route("/health", health),
], lifespan=lifespan)
