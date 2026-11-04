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

## Roadmap

- [x] **Phase 0 - Setup:** fake replica, benchmark, plots
- [ ] **Phase 1 - Prefill, decode, batching on a real GPU** (vLLM): batch knee, prompt length vs TTFT, roofline check
- [ ] **Phase 2 - KV cache:** prefix caching on/off, multi-turn TTFT, preemption under memory pressure
- [ ] **Phase 3 - Gateway + router:** token-bucket limits per tenant, tier queues with deadlines, 429/529 shedding, cache-aware routing, cancel on disconnect
- [ ] **Phase 4 - Async + batch API:** 202 + polling, durable queue, pull-based dispatcher, idempotent results
- [ ] **Phase 5 - Observability:** Prometheus/Grafana, latency per layer, p95 debugging experiments
- [ ] **Phase 6 - Autoscaling + cold start:** Kubernetes, concurrency target, warm pool, canary

## Layout

```
fake_replica/server.py   OpenAI-compatible fake GPU replica (continuous batching model)
bench/bench.py           concurrency sweep: TTFT, time per token, throughput
bench/plot.py            charts: throughput vs per-user speed, TTFT
docs/gpu_setup.md        renting a GPU and running vLLM against the same benchmark
results/                 CSVs and PNGs from your runs
```
