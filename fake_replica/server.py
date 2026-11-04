"""
Fake LLM replica: an OpenAI-compatible server that *behaves* like a GPU
running continuous batching, without needing a GPU.

Why: lets you build and test the platform layers (gateway, router, shedding,
autoscaling) on a laptop, and see the latency/throughput trade-off of batching.

Model of a GPU replica (simplified, but the shapes are right):
  * One engine loop runs "steps". Every step, each running request gets
    one new token (continuous batching: requests join/leave every step).
  * Step time  = base_ms + per_seq_ms * running + prefill cost of new joiners
      - base_ms     ~ reading the model weights once per step (memory-bound)
      - per_seq_ms  ~ extra cost per request in the batch (KV reads, etc.)
      - prefill     ~ prefill_ms_per_1k * new prompt tokens / 1000
  * KV memory is limited: a request reserves prompt_tokens + max_tokens.
    If it doesn't fit, it waits. If the wait queue is full -> 503.

Run:
  python -m uvicorn fake_replica.server:app --port 8001
  # tune with env vars, e.g. MAX_BATCH=64 BASE_MS=20 PER_SEQ_MS=0.25
"""

import asyncio
import contextlib
import json
import os
import time
import uuid

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Route


def _env(name, default, cast=float):
    return cast(os.environ.get(name, default))


MODEL_NAME = os.environ.get("MODEL_NAME", "fake-8b")
MAX_BATCH = _env("MAX_BATCH", 64, int)            # max running requests per step
BASE_MS = _env("BASE_MS", 20.0)                   # weight read per step
PER_SEQ_MS = _env("PER_SEQ_MS", 0.25)             # extra per running request
PREFILL_MS_PER_1K = _env("PREFILL_MS_PER_1K", 40.0)
KV_CAPACITY_TOKENS = _env("KV_CAPACITY_TOKENS", 200_000, int)
MAX_WAITING = _env("MAX_WAITING", 256, int)       # queue limit before 503


def count_tokens(text: str) -> int:
    # Rough rule of thumb: ~4 characters per token.
    return max(1, len(text) // 4)


class Seq:
    def __init__(self, prompt_tokens: int, max_tokens: int):
        self.id = uuid.uuid4().hex[:12]
        self.prompt_tokens = prompt_tokens
        self.max_tokens = max_tokens
        self.generated = 0
        self.prefilled = False
        self.cancelled = False
        self.out: asyncio.Queue = asyncio.Queue()

    @property
    def kv_reserved(self) -> int:
        return self.prompt_tokens + self.max_tokens


class Engine:
    def __init__(self):
        self.waiting: list[Seq] = []
        self.running: list[Seq] = []
        self.kv_used = 0
        self.wake = asyncio.Event()
        self.stats = {"steps": 0, "tokens_generated": 0, "rejected": 0,
                      "completed": 0, "cancelled": 0}

    def submit(self, seq: Seq) -> bool:
        if len(self.waiting) >= MAX_WAITING or seq.kv_reserved > KV_CAPACITY_TOKENS:
            self.stats["rejected"] += 1
            return False
        self.waiting.append(seq)
        self.wake.set()
        return True

    def _admit(self) -> int:
        """Move waiting requests into the running batch while slots and KV allow.
        Returns the number of prompt tokens that must be prefilled this step."""
        new_prefill = 0
        still_waiting = []
        for seq in self.waiting:
            if seq.cancelled:
                continue
            fits = (len(self.running) < MAX_BATCH
                    and self.kv_used + seq.kv_reserved <= KV_CAPACITY_TOKENS)
            if fits:
                self.running.append(seq)
                self.kv_used += seq.kv_reserved
                new_prefill += seq.prompt_tokens
            else:
                still_waiting.append(seq)
        self.waiting = still_waiting
        return new_prefill

    def _finish(self, seq: Seq, reason: str):
        self.running.remove(seq)
        self.kv_used -= seq.kv_reserved
        if reason == "cancelled":
            self.stats["cancelled"] += 1
        else:
            self.stats["completed"] += 1
        seq.out.put_nowait(("done", reason))

    async def run(self):
        while True:
            if not self.waiting and not self.running:
                self.wake.clear()
                await self.wake.wait()

            new_prefill = self._admit()
            step_ms = (BASE_MS + PER_SEQ_MS * len(self.running)
                       + PREFILL_MS_PER_1K * new_prefill / 1000.0)
            await asyncio.sleep(step_ms / 1000.0)
            self.stats["steps"] += 1

            for seq in list(self.running):
                if seq.cancelled:
                    self._finish(seq, "cancelled")
                    continue
                # Prefill produces the first token; each later step produces one more.
                seq.prefilled = True
                seq.generated += 1
                self.stats["tokens_generated"] += 1
                seq.out.put_nowait(("token", f" tok{seq.generated}"))
                if seq.generated >= seq.max_tokens:
                    self._finish(seq, "length")


engine = Engine()


def _prompt_text(body: dict) -> str:
    parts = []
    for m in body.get("messages", []):
        c = m.get("content", "")
        if isinstance(c, list):  # OpenAI content parts
            c = " ".join(p.get("text", "") for p in c if isinstance(p, dict))
        parts.append(str(c))
    if "prompt" in body:
        parts.append(str(body["prompt"]))
    return "\n".join(parts)


async def chat_completions(request: Request):
    body = await request.json()
    prompt_tokens = count_tokens(_prompt_text(body))
    max_tokens = int(body.get("max_tokens") or body.get("max_completion_tokens") or 256)
    stream = bool(body.get("stream", False))
    include_usage = bool((body.get("stream_options") or {}).get("include_usage", False))

    seq = Seq(prompt_tokens, max_tokens)
    if not engine.submit(seq):
        return JSONResponse(
            {"error": {"type": "overloaded", "message": "replica is full, retry later"}},
            status_code=503, headers={"Retry-After": "1"})

    created = int(time.time())
    req_id = f"chatcmpl-{seq.id}"

    def chunk(delta: dict, finish=None):
        return {"id": req_id, "object": "chat.completion.chunk", "created": created,
                "model": MODEL_NAME,
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}

    usage = lambda: {"prompt_tokens": prompt_tokens, "completion_tokens": seq.generated,
                     "total_tokens": prompt_tokens + seq.generated}

    if not stream:
        text = []
        while True:
            kind, val = await seq.out.get()
            if kind == "done":
                break
            text.append(val)
        return JSONResponse({
            "id": req_id, "object": "chat.completion", "created": created,
            "model": MODEL_NAME,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "".join(text)},
                         "finish_reason": val}],
            "usage": usage()})

    async def events():
        try:
            yield f"data: {json.dumps(chunk({'role': 'assistant'}))}\n\n"
            while True:
                kind, val = await seq.out.get()
                if kind == "done":
                    yield f"data: {json.dumps(chunk({}, finish=val))}\n\n"
                    break
                yield f"data: {json.dumps(chunk({'content': val}))}\n\n"
            if include_usage:
                u = {"id": req_id, "object": "chat.completion.chunk", "created": created,
                     "model": MODEL_NAME, "choices": [], "usage": usage()}
                yield f"data: {json.dumps(u)}\n\n"
            yield "data: [DONE]\n\n"
        except asyncio.CancelledError:
            # Client disconnected: stop generating and free the KV reservation.
            seq.cancelled = True
            raise

    return StreamingResponse(events(), media_type="text/event-stream")


async def models(_):
    return JSONResponse({"object": "list", "data": [{"id": MODEL_NAME, "object": "model"}]})


async def health(_):
    return PlainTextResponse("ok")


async def metrics(_):
    s = engine.stats
    lines = [
        f"fake_running_requests {len(engine.running)}",
        f"fake_waiting_requests {len(engine.waiting)}",
        f"fake_kv_used_tokens {engine.kv_used}",
        f"fake_kv_capacity_tokens {KV_CAPACITY_TOKENS}",
        f"fake_steps_total {s['steps']}",
        f"fake_generated_tokens_total {s['tokens_generated']}",
        f"fake_requests_completed_total {s['completed']}",
        f"fake_requests_cancelled_total {s['cancelled']}",
        f"fake_requests_rejected_total {s['rejected']}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n")


@contextlib.asynccontextmanager
async def lifespan(_app):
    task = asyncio.get_running_loop().create_task(engine.run())
    yield
    task.cancel()


app = Starlette(
    routes=[
        Route("/v1/chat/completions", chat_completions, methods=["POST"]),
        Route("/v1/models", models),
        Route("/health", health),
        Route("/metrics", metrics),
    ],
    lifespan=lifespan,
)
