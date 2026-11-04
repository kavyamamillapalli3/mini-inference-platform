"""
Benchmark an OpenAI-compatible endpoint (vLLM, SGLang, or the fake replica).

For each concurrency level it fires requests and measures, per request:
  * TTFT   - time to first token (queue wait + prefill)
  * TPOT   - time per output token after the first (decode speed per user)
  * E2E    - end-to-end time
and per level:
  * throughput - total output tokens / wall-clock seconds

Example:
  python bench/bench.py --url http://localhost:8001 --model fake-8b \
      --concurrency 1,4,16,64 --input-tokens 512 --output-tokens 128 \
      --out results/fake_sweep.csv
"""

import argparse
import csv
import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import requests


def make_prompt(n_tokens: int) -> str:
    # "word " is roughly 1 token for most tokenizers (and ~1.25 for the fake server).
    return ("word " * n_tokens).strip()


def one_request(url, model, prompt, output_tokens, api_key, timeout,
                system=None, extra_headers=None):
    messages = [{"role": "user", "content": prompt}]
    if system:
        messages.insert(0, {"role": "system", "content": system})
    body = {
        "model": model,
        "messages": messages,
        "max_tokens": output_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
        "ignore_eos": True,      # vLLM/SGLang: force exactly max_tokens (ignored elsewhere)
        "temperature": 0,
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    headers.update(extra_headers or {})

    t0 = time.perf_counter()
    first = last = None
    chunks = 0
    usage_tokens = None
    status = 0
    try:
        with requests.post(f"{url}/v1/chat/completions", json=body, headers=headers,
                           stream=True, timeout=timeout) as r:
            status = r.status_code
            if r.status_code != 200:
                return {"ok": False, "status": status, "e2e": time.perf_counter() - t0}
            buf = b""
            for piece in r.iter_content(chunk_size=None):
                buf += piece
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line.startswith(b"data:"):
                        continue
                    data = line[5:].strip()
                    if data == b"[DONE]":
                        continue
                    msg = json.loads(data)
                    if msg.get("error"):
                        # e.g. replica died mid-stream: an explicit failure
                        return {"ok": False, "status": "midstream_error",
                                "e2e": time.perf_counter() - t0}
                    if msg.get("usage"):
                        usage_tokens = msg["usage"].get("completion_tokens")
                    for ch in msg.get("choices", []):
                        if ch.get("delta", {}).get("content"):
                            now = time.perf_counter()
                            if first is None:
                                first = now
                            last = now
                            chunks += 1
    except requests.RequestException as e:
        return {"ok": False, "status": status or -1, "error": str(e),
                "e2e": time.perf_counter() - t0}

    if first is None:
        return {"ok": False, "status": status, "e2e": time.perf_counter() - t0}
    n_out = usage_tokens or chunks
    return {
        "ok": True,
        "status": status,
        "ttft": first - t0,
        "tpot": (last - first) / max(1, n_out - 1),
        "e2e": last - t0,
        "out_tokens": n_out,
    }


def pct(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    k = min(len(values) - 1, max(0, round(p / 100 * (len(values) - 1))))
    return values[k]


def run_level(args, concurrency, prompt):
    n = max(args.min_requests, concurrency * args.requests_per_slot)
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(one_request, args.url, args.model, prompt,
                               args.output_tokens, args.api_key, args.timeout)
                   for _ in range(n)]
        results = [f.result() for f in futures]
    wall = time.perf_counter() - start

    ok = [r for r in results if r["ok"]]
    ttft = [r["ttft"] for r in ok]
    tpot = [r["tpot"] for r in ok]
    e2e = [r["e2e"] for r in ok]
    out_tokens = sum(r["out_tokens"] for r in ok)
    return {
        "concurrency": concurrency,
        "requests": n,
        "ok": len(ok),
        "errors": n - len(ok),
        "throughput_tok_s": round(out_tokens / wall, 1),
        "ttft_p50_ms": round(pct(ttft, 50) * 1000, 1),
        "ttft_p95_ms": round(pct(ttft, 95) * 1000, 1),
        "tpot_p50_ms": round(pct(tpot, 50) * 1000, 2),
        "tpot_p95_ms": round(pct(tpot, 95) * 1000, 2),
        "e2e_p50_s": round(pct(e2e, 50), 2),
        "e2e_p95_s": round(pct(e2e, 95), 2),
        "per_user_tok_s": round(1 / statistics.median(tpot), 1) if tpot else 0,
        "input_tokens": args.input_tokens,
        "output_tokens": args.output_tokens,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8001")
    ap.add_argument("--model", default="fake-8b")
    ap.add_argument("--api-key", default=os.environ.get("OPENAI_API_KEY", ""))
    ap.add_argument("--concurrency", default="1,4,16,64")
    ap.add_argument("--input-tokens", type=int, default=512)
    ap.add_argument("--output-tokens", type=int, default=128)
    ap.add_argument("--requests-per-slot", type=int, default=3,
                    help="requests per level = concurrency x this (min --min-requests)")
    ap.add_argument("--min-requests", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=600)
    ap.add_argument("--out", default="results/sweep.csv")
    args = ap.parse_args()

    prompt = make_prompt(args.input_tokens)
    levels = [int(x) for x in args.concurrency.split(",")]
    rows = []
    print(f"{'conc':>5} {'ok':>5} {'err':>4} {'tok/s':>9} {'TTFT p50':>9} {'TTFT p95':>9} "
          f"{'TPOT p50':>9} {'user tok/s':>10}")
    for c in levels:
        row = run_level(args, c, prompt)
        rows.append(row)
        print(f"{c:>5} {row['ok']:>5} {row['errors']:>4} {row['throughput_tok_s']:>9} "
              f"{row['ttft_p50_ms']:>8}ms {row['ttft_p95_ms']:>8}ms "
              f"{row['tpot_p50_ms']:>8}ms {row['per_user_tok_s']:>10}")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"\nSaved {args.out}")


if __name__ == "__main__":
    main()
