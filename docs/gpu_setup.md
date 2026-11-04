# Running the benchmark against a real GPU (Phase 1)

You only need a GPU for Phases 1-2. Everything else can use the fake replica.

## 1. Rent a GPU

Any provider with hourly NVIDIA GPUs works (RunPod, Lambda, Modal, AWS g5/g6, GCP L4).

| GPU | Memory | Good for |
|---|---|---|
| L4 / A10G | 24 GB | 7-8B models with short contexts; cheapest way to start |
| A100 80GB / H100 | 80 GB | Longer contexts, bigger batches, the roofline experiment |

Check current hourly prices, and **stop the instance after each session.**

## 2. Start vLLM

```bash
pip install vllm
# Qwen2.5-7B-Instruct needs no license approval; Llama models need a Hugging Face token
vllm serve Qwen/Qwen2.5-7B-Instruct --max-model-len 8192 --port 8000
```

vLLM exposes the same OpenAI-compatible `/v1/chat/completions` endpoint as the fake replica,
plus Prometheus metrics at `/metrics` (KV cache usage, running and waiting requests,
prefix cache hits).

To reach it from your laptop, use the provider's public port, or an SSH tunnel:

```bash
ssh -L 8000:localhost:8000 user@<gpu-host>
```

## 3. Run the same sweep

```bash
python bench/bench.py --url http://localhost:8000 --model Qwen/Qwen2.5-7B-Instruct \
    --concurrency 1,4,16,64,128,256 --input-tokens 512 --output-tokens 128 \
    --out results/vllm_l4_sweep.csv
python bench/plot.py results/vllm_l4_sweep.csv --slo-tpot-ms 50
```

## Phase 1 experiments

1. **Batch knee:** the sweep above. Where does time per token cross your 50 ms SLO?
2. **Prompt length vs TTFT:** run `--concurrency 1` with `--input-tokens 128,1024,4096`
   (one run each). TTFT should grow with input; time per token should barely move.
3. **Roofline check:** at concurrency 1, time per token should be close to
   `weight bytes / memory bandwidth` (e.g. 7B x 2 bytes = ~15 GB; on an L4 at ~300 GB/s
   that is ~50 ms; on an H100 at ~3.35 TB/s it is ~5 ms). Compare with your measurement.
4. **vLLM's own metrics:** `curl localhost:8000/metrics | grep -E "kv_cache|num_requests"`
   during a run, to watch KV usage climb with concurrency.
