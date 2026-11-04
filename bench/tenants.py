"""
Noisy-neighbour test: two tenants share one replica.

A Free tenant floods with many parallel workers; a Paid tenant sends normal
traffic. Run it twice:
  --direct  : both hit the replica directly (no gateway) -> Paid suffers
  (default) : both go through the gateway -> Free is throttled, Paid is fine

  python bench/tenants.py --url http://localhost:8001 --direct
  python bench/tenants.py --url http://localhost:8000
"""

import argparse
import os
import sys
import threading
import time
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from bench.bench import make_prompt, one_request, pct  # noqa: E402


def worker(name, key, args, prompt, stop_at, results, lock):
    while time.perf_counter() < stop_at:
        r = one_request(args.url, args.model, prompt, args.output_tokens,
                        "" if args.direct else key, args.timeout)
        with lock:
            results[name].append(r)
        if not r["ok"] and r.get("status") in (429, 529, 503):
            time.sleep(args.backoff)      # simple client backoff after a rejection


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--model", default="fake-8b")
    ap.add_argument("--direct", action="store_true", help="skip the gateway (no API keys)")
    ap.add_argument("--free-workers", type=int, default=96)
    ap.add_argument("--paid-workers", type=int, default=8)
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--input-tokens", type=int, default=512)
    ap.add_argument("--output-tokens", type=int, default=64)
    ap.add_argument("--backoff", type=float, default=0.5)
    ap.add_argument("--timeout", type=float, default=120)
    args = ap.parse_args()

    prompt = make_prompt(args.input_tokens)
    results, lock = defaultdict(list), threading.Lock()
    stop_at = time.perf_counter() + args.duration
    threads = []
    for name, key, n in [("free", "key-free-1", args.free_workers),
                         ("paid", "key-paid-1", args.paid_workers)]:
        for _ in range(n):
            t = threading.Thread(target=worker,
                                 args=(name, key, args, prompt, stop_at, results, lock))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()

    mode = "DIRECT (no gateway)" if args.direct else "THROUGH GATEWAY"
    print(f"\n{mode}: {args.free_workers} free workers vs {args.paid_workers} paid workers, "
          f"{args.duration:.0f}s")
    print(f"{'tenant':>6} {'ok':>6} {'429':>5} {'5xx':>5} {'TTFT p50':>10} {'TTFT p95':>10} "
          f"{'TPOT p50':>9} {'tok/s':>8}")
    for name in ("free", "paid"):
        rs = results[name]
        ok = [r for r in rs if r["ok"]]
        n429 = sum(1 for r in rs if r.get("status") == 429)
        n5xx = sum(1 for r in rs if r.get("status", 0) >= 500 or r.get("status") == -1)
        ttft = [r["ttft"] for r in ok]
        tpot = [r["tpot"] for r in ok]
        toks = sum(r["out_tokens"] for r in ok) / args.duration
        print(f"{name:>6} {len(ok):>6} {n429:>5} {n5xx:>5} {pct(ttft, 50)*1000:>9.0f}ms "
              f"{pct(ttft, 95)*1000:>9.0f}ms {pct(tpot, 50)*1000:>8.1f}ms {toks:>8.0f}")


if __name__ == "__main__":
    main()
