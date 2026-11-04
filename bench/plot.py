"""
Plot a concurrency sweep: total throughput vs per-user speed, and TTFT.

  python bench/plot.py results/fake_sweep.csv --slo-tpot-ms 50
"""

import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--slo-tpot-ms", type=float, default=50.0,
                    help="draw your time-per-token SLO line")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    df = pd.read_csv(args.csv).sort_values("concurrency")
    out = args.out or args.csv.replace(".csv", ".png")

    fig, (ax1, ax3) = plt.subplots(1, 2, figsize=(12, 4.5))

    ax1.plot(df.concurrency, df.throughput_tok_s, marker="o", label="total throughput (tok/s)")
    ax1.set_xscale("log", base=2)
    ax1.set_xlabel("concurrent requests (batch size)")
    ax1.set_ylabel("total output tokens / s")
    ax2 = ax1.twinx()
    ax2.plot(df.concurrency, df.tpot_p50_ms, marker="s", color="tab:red",
             label="time per output token p50 (ms)")
    ax2.axhline(args.slo_tpot_ms, color="tab:red", linestyle="--", linewidth=1)
    ax2.set_ylabel("time per output token (ms)")
    ax1.set_title("Batching: throughput rises, per-user speed falls")
    lines = ax1.get_legend_handles_labels()[0] + ax2.get_legend_handles_labels()[0]
    labels = ax1.get_legend_handles_labels()[1] + ax2.get_legend_handles_labels()[1]
    ax1.legend(lines, labels, loc="upper left", fontsize=8)

    ax3.plot(df.concurrency, df.ttft_p50_ms, marker="o", label="TTFT p50")
    ax3.plot(df.concurrency, df.ttft_p95_ms, marker="o", label="TTFT p95")
    ax3.set_xscale("log", base=2)
    ax3.set_xlabel("concurrent requests")
    ax3.set_ylabel("ms")
    ax3.set_title("Time to first token")
    ax3.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(out, dpi=130)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
