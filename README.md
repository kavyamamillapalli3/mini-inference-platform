# mini-inference-platform

A small, hands-on LLM inference platform built in layers, to measure for yourself
what batching, KV cache, admission control, load shedding and autoscaling actually do.

It works **without a GPU** (a fake replica that behaves like continuous batching)
and **with a GPU** (point the same benchmark at vLLM or SGLang).

## Quick start (no GPU, on your laptop)

```bash
cd mini-inference-platform
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# terminal 1: fake GPU replica (OpenAI-compatible, streaming)
python -m uvicorn fake_replica.server:app --port 8001

# terminal 2: sweep batch sizes, then plot
python bench/bench.py --url http://localhost:8001 --model fake-8b \
    --concurrency 1,4,16,64,128 --input-tokens 512 --output-tokens 64 \
    --out results/fake_sweep.csv
python bench/plot.py results/fake_sweep.csv --slo-tpot-ms 50
open results/fake_sweep.png
```

Tune the fake replica with environment variables, e.g.
`MAX_BATCH=32 BASE_MS=20 PER_SEQ_MS=0.5 KV_CAPACITY_TOKENS=50000`.
It also serves `/metrics` (running, waiting, KV used, rejected, cancelled).

## What the first sweep shows (fake replica, default settings)

| Concurrency | Throughput (tok/s) | TTFT p50 | Time per token p50 | Per-user tok/s |
|---|---|---|---|---|
| 1 | 46 | 48 ms | 21 ms | 47 |
| 4 | 171 | 145 ms | 22 ms | 47 |
| 16 | 506 | 449 ms | 25 ms | 40 |
| 64 | 1,017 | 1,050 ms | 47 ms | 21 |
| 128 | 1,022 | 4,829 ms | 50 ms | 20 |

- **Batching is nearly free at first:** 1 → 16 requests gives ~11x throughput for ~4 ms more per token.
- **Then the SLO breaks:** at 64 the time per token is ~47 ms, close to a 50 ms target.
- **Past the batch limit (64), extra requests just queue:** throughput is flat and TTFT explodes.
  That is exactly why admission control and load shedding exist.

## Gateway (Phase 3a): per-tenant admission control

```bash
# terminal 1: replica
python -m uvicorn fake_replica.server:app --port 8001 --log-level warning
# terminal 2: gateway in front of it
UPSTREAM=http://localhost:8001 python -m uvicorn gateway.server:app --port 8000 --log-level warning
# terminal 3: noisy-neighbour test, without and with the gateway
python bench/tenants.py --url http://localhost:8001 --direct --duration 20
python bench/tenants.py --url http://localhost:8000 --duration 20
curl -s localhost:8000/metrics
```

The gateway checks, in order: API key (401) -> size caps per tier (400) ->
concurrency limit (429) -> token bucket in tokens/min, charging prompt + max_tokens
up front and refunding unused tokens at the end (429 + Retry-After) -> proxy with
streaming. A replica 503 becomes a 529 "overloaded" for the client. Tenants live in
`gateway/tenants.json`.

**Result:** a Free tenant flooding with 96 workers pushed Paid's TTFT to ~4 s
without the gateway; with it, Paid's TTFT dropped to ~0.32 s (~12x) and Paid
completed 4x more requests. Details: `results/noisy_neighbour.md`.

## Router (Phase 3b): where and when a request runs

```bash
./scripts/start.sh 4          # 4 fake replicas (8011-8014) + router (8002) + gateway (8000)
python bench/router_tests.py tiers       # Paid vs Free under overload
python bench/router_tests.py affinity    # cache-aware vs least-loaded routing
python bench/router_tests.py failure     # then kill a replica: kill $(lsof -ti tcp:8012)
curl -s localhost:8002/metrics
./scripts/stop.sh
# compare settings by restarting with env vars, e.g.
POLICY=fifo ./scripts/start.sh 2
ROUTING=least_loaded ./scripts/start.sh 4
```

The router keeps bounded in-memory queues per tier with deadlines, dispatches with
weighted round robin (4 Paid : 1 Free) and **reserves capacity** (Free uses at most 60%
of slots). It picks a healthy replica with a free slot, preferring the one that already
caches the request's system prompt (rendezvous hashing), else the least loaded.
Health checks eject dead replicas; a replica failing before any token is retried once
elsewhere, one failing mid-stream yields an explicit error event.

**Results** (`results/router_results.md`):
- Priority + reserved capacity: Paid went from 73 rejections to **0** and ~1.9x throughput vs FIFO.
- Cache-aware routing: prefix hit rate **21% -> 83%**, median TTFT **627 ms -> 48 ms**.
- Killing a replica: 12 in-flight streams got clean errors, the rest moved to the survivor, 0 rejections.

## Roadmap

- [x] **Phase 0 - Setup:** fake replica, benchmark, plots
- [ ] **Phase 1 - Prefill, decode, batching on a real GPU** (vLLM): batch knee, prompt length vs TTFT, roofline check
- [ ] **Phase 2 - KV cache:** prefix caching on/off, multi-turn TTFT, preemption under memory pressure
- [x] **Phase 3a - Gateway:** API keys, token bucket per tenant, concurrency limits, size caps, 429/529, streaming proxy
- [x] **Phase 3b - Router:** tier queues with deadlines, weighted priority + reserved capacity, cache-aware routing, health checks, mid-stream failure handling
- [ ] **Phase 3c - Chunked prefill in the fake replica:** fix the ~1 s TTFT from prefill bursts
- [ ] **Phase 4 - Async + batch API:** 202 + polling, durable queue, pull-based dispatcher, idempotent results
- [ ] **Phase 5 - Observability:** Prometheus/Grafana, latency per layer, p95 debugging experiments
- [ ] **Phase 6 - Autoscaling + cold start:** Kubernetes, concurrency target, warm pool, canary

## Layout

```
fake_replica/server.py   OpenAI-compatible fake GPU replica (continuous batching model)
gateway/server.py        front door: auth, tier caps, concurrency + token-bucket limits, streaming proxy
gateway/limits.py        token bucket and tenant state
gateway/upstream.py      tiny streaming HTTP client (closing it cancels generation upstream)
gateway/tenants.json     API keys -> tenant, tier and limits
bench/bench.py           concurrency sweep: TTFT, time per token, throughput
bench/tenants.py         noisy-neighbour test: Free flood vs Paid, direct or via gateway
router/server.py         tier queues, deadlines, reserved capacity, cache-aware routing, health checks
bench/router_tests.py    router experiments: tiers, affinity, failure
scripts/start.sh, stop.sh  start/stop N replicas + router + gateway
bench/plot.py            charts: throughput vs per-user speed, TTFT
docs/gpu_setup.md        renting a GPU and running vLLM against the same benchmark
results/                 CSVs and PNGs from your runs
```
