# Noisy neighbour: gateway on vs off

Setup: one fake replica (MAX_BATCH=64). A Free tenant floods with 96 parallel
workers; a Paid tenant sends normal traffic with 8 workers. 512-token prompts,
64 output tokens, 20 seconds each run.

| Run | Tenant | Completed | 429s | TTFT p50 | TTFT p95 | Time per token p50 | Output tok/s |
|---|---|---|---|---|---|---|---|
| Direct (no gateway) | Free | 337 | 0 | 3,992 ms | 4,949 ms | 48.4 ms | 1,078 |
| Direct (no gateway) | **Paid** | **24** | 0 | **4,009 ms** | **5,589 ms** | 48.4 ms | **77** |
| Through gateway | Free | 37 | 3,706 | 145 ms | 151 ms | 26.9 ms | 118 |
| Through gateway | **Paid** | **96** | 0 | **322 ms** | **326 ms** | 24.0 ms | **307** |

Free tenant limits: 20K tokens/min, 4 concurrent. Its 3,706 rejections were
2,832 concurrency-limit and 874 token-rate-limit 429s.

**What it shows**

- Without admission control, one tenant fills every batch slot and the queue,
  so everyone (including Paid) waits ~4 s for a first token.
- With per-tenant limits, the flood is rejected at the door in milliseconds,
  the replica stays below saturation, and Paid gets **~12x better TTFT** and
  **4x more completed requests**.
- Rejections are cheap: a 429 costs the gateway microseconds and the GPU nothing.
