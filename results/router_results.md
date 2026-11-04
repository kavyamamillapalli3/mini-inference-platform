# Router experiments (Phase 3b)

All runs use fake replicas (MAX_BATCH=64) behind the router, 20 seconds each.

## 1. Tiers: priority + reserved capacity vs one FIFO queue

2 replicas x 48 slots. 30 Paid workers and 150 Free workers overload the system.
Deadlines: Paid 3 s, Free 1 s. Priority mode = weighted round robin (4 Paid : 1 Free)
plus Free capped at 60% of slots.

| Policy | Tier | Completed | 529s | TTFT p50 | Output tok/s |
|---|---|---|---|---|---|
| FIFO (one queue) | Paid | 112 | 73 | 1,312 ms | 358 |
| FIFO (one queue) | Free | 504 | 1,564 | 1,297 ms | 1,613 |
| Priority + reserve | **Paid** | **210** | **0** | 1,100 ms | **672** |
| Priority + reserve | Free | 406 | 2,728 | 1,169 ms | 1,299 |

- **Priority alone was not enough.** A first version without the Free cap still
  expired 37 Paid requests: once Free fills every slot, reordering the queue doesn't
  help, because running work isn't preempted. **Reserving capacity** (Free <= 60% of
  slots) fixed it: Paid got zero rejections and ~1.9x the throughput of FIFO.
- **TTFT is still ~1.1 s for everyone:** dozens of requests join a replica at once and
  their prefill runs in one long step (no chunked prefill). That's the next lever.

## 2. Routing: cache-aware (affinity) vs least-loaded

4 replicas, each with an 8-entry prefix cache. 32 "apps", each with its own
2,000-token system prompt; 32 workers pick an app at random.

| Routing | Prefix cache hit rate | TTFT p50 | TTFT p95 | Output tok/s |
|---|---|---|---|---|
| Least-loaded | 21% | 627 ms | 818 ms | 1,024 |
| **Affinity** (rendezvous hash of the system prompt) | **83%** | **48 ms** | 374 ms | **1,245** |

- Affinity sends each app to "its" replica, so its system prompt stays cached:
  **13x lower median TTFT** and 22% more throughput.
- **Tradeoff:** load was uneven (two replicas got ~134 requests, two got ~60),
  and hits weren't 100% because some replicas owned more than 8 apps. Real routers
  balance affinity against load and cache size.

## 3. Failure: kill a replica mid-stream

2 replicas, 24 Paid workers, 128-token answers; replica 8012 killed with `kill -9` at t = 8 s.

| Completed | Mid-stream errors | 529s | TTFT p50 |
|---|---|---|---|
| 132 | 12 | 0 | 354 ms |

- The 12 streams in flight on the dead replica got an **explicit error event**
  (`replica_failed`), **not a silent retry** (a retry would produce a different answer
  after the user already saw part of one).
- The router marked the replica unhealthy and sent all later traffic to the survivor;
  no request was rejected.
