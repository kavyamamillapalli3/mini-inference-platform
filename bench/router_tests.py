"""
Router experiments (send traffic straight to the router, with an X-Tier header).

  tiers     Paid and Free share overloaded replicas. Compare POLICY=priority vs fifo.
            python bench/router_tests.py tiers --paid-workers 30 --free-workers 150

  affinity  Many "apps", each with its own long system prompt. Compare
            ROUTING=affinity vs least_loaded: prefix-cache hit rate and TTFT.
            python bench/router_tests.py affinity --replicas http://localhost:8011,...

  failure   Steady traffic; kill a replica while it runs and watch the errors.
            python bench/router_tests.py failure --duration 30
"""

import argparse
import os
import random
import sys
import threading
import time
from collections import defaultdict

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from bench.bench import make_prompt, one_request, pct  # noqa: E402


def run_workers(groups, args, duration):
    """groups: list of (name, n_workers, fn) where fn() -> one_request result."""
    results, lock = defaultdict(list), threading.Lock()
    stop_at = time.perf_counter() + duration

    def loop(name, fn):
        while time.perf_counter() < stop_at:
            r = fn()
            with lock:
                results[name].append(r)
            if not r["ok"]:
                time.sleep(args.backoff)

    threads = [threading.Thread(target=loop, args=(name, fn))
               for name, n, fn in groups for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def report(results, duration, title):
    print(f"\n{title}")
    print(f"{'group':>8} {'ok':>6} {'529':>5} {'midstr':>6} {'other':>5} {'TTFT p50':>10} "
          f"{'TTFT p95':>10} {'TPOT p50':>9} {'tok/s':>7}")
    for name, rs in results.items():
        ok = [r for r in rs if r["ok"]]
        n529 = sum(1 for r in rs if r.get("status") == 529)
        nmid = sum(1 for r in rs if r.get("status") == "midstream_error")
        other = len(rs) - len(ok) - n529 - nmid
        ttft = [r["ttft"] for r in ok]
        tpot = [r["tpot"] for r in ok]
        toks = sum(r["out_tokens"] for r in ok) / duration
        print(f"{name:>8} {len(ok):>6} {n529:>5} {nmid:>6} {other:>5} "
              f"{pct(ttft, 50)*1000:>9.0f}ms {pct(ttft, 95)*1000:>9.0f}ms "
              f"{pct(tpot, 50)*1000:>8.1f}ms {toks:>7.0f}")


def scrape(url):
    out = {}
    try:
        for line in requests.get(f"{url}/metrics", timeout=2).text.splitlines():
            if line and not line.startswith("#"):
                k, _, v = line.rpartition(" ")
                out[k] = float(v)
    except requests.RequestException:
        pass
    return out


def tiers(args):
    prompt = make_prompt(args.input_tokens)

    def call(tier):
        return lambda: one_request(args.url, args.model, prompt, args.output_tokens, "",
                                   args.timeout, extra_headers={"X-Tier": tier})

    res = run_workers([("paid", args.paid_workers, call("paid")),
                       ("free", args.free_workers, call("free"))], args, args.duration)
    report(res, args.duration, f"TIERS: {args.paid_workers} paid vs {args.free_workers} "
                               f"free workers, {args.duration:.0f}s")


def affinity(args):
    apps = [f"You are assistant #{i} for company {i}. " + ("Follow the policy. " *
            (args.system_tokens // 4)) for i in range(args.apps)]
    user = make_prompt(32)
    replicas = [u for u in args.replicas.split(",") if u]
    before = {u: scrape(u) for u in replicas}

    def call():
        return one_request(args.url, args.model, user, args.output_tokens, "", args.timeout,
                           system=random.choice(apps), extra_headers={"X-Tier": "paid"})

    res = run_workers([("all", args.workers, call)], args, args.duration)
    report(res, args.duration, f"AFFINITY: {args.apps} apps x {args.system_tokens}-token "
                               f"system prompts, {args.workers} workers")
    hits = misses = 0
    for u in replicas:
        after = scrape(u)
        hits += after.get("fake_prefix_cache_hits_total", 0) - \
            before[u].get("fake_prefix_cache_hits_total", 0)
        misses += after.get("fake_prefix_cache_misses_total", 0) - \
            before[u].get("fake_prefix_cache_misses_total", 0)
    if hits + misses:
        print(f"prefix cache hit rate across replicas: {hits / (hits + misses):.0%} "
              f"({int(hits)} hits, {int(misses)} misses)")


def failure(args):
    prompt = make_prompt(args.input_tokens)

    def call():
        return one_request(args.url, args.model, prompt, args.output_tokens, "",
                           args.timeout, extra_headers={"X-Tier": "paid"})

    print(f"Sending steady traffic for {args.duration:.0f}s. Kill one replica now, e.g.\n"
          f"  kill $(lsof -ti tcp:8012)")
    res = run_workers([("paid", args.workers, call)], args, args.duration)
    report(res, args.duration, "FAILURE TEST")
    print("\nrouter metrics:")
    for k, v in scrape(args.url).items():
        if any(s in k for s in ("healthy", "retries", "midstream", "dispatched_total{")):
            print(f"  {k} {int(v)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", choices=["tiers", "affinity", "failure"])
    ap.add_argument("--url", default="http://localhost:8002")
    ap.add_argument("--model", default="fake-8b")
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--input-tokens", type=int, default=512)
    ap.add_argument("--output-tokens", type=int, default=64)
    ap.add_argument("--paid-workers", type=int, default=30)
    ap.add_argument("--free-workers", type=int, default=150)
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--apps", type=int, default=32)
    ap.add_argument("--system-tokens", type=int, default=2000)
    ap.add_argument("--replicas", default="http://localhost:8011,http://localhost:8012,"
                                          "http://localhost:8013,http://localhost:8014")
    ap.add_argument("--backoff", type=float, default=0.2)
    ap.add_argument("--timeout", type=float, default=120)
    args = ap.parse_args()
    {"tiers": tiers, "affinity": affinity, "failure": failure}[args.scenario](args)


if __name__ == "__main__":
    main()
