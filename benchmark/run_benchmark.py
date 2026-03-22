"""
Raven LoCoMo Benchmark

Usage:
    python -m benchmark.run_benchmark [--mode standard|adversarial|both]
                                      [--max-examples 50]
                                      [--model qwen3.5:4b]
                                      [--judge kimi]
                                      [--n-recent 10]
                                      [--output results/]

Inference backends:
    --model qwen3.5:4b     Use Ollama (local, default)
    --model kimi-api       Use Kimi K2.5 API (better judge quality)

Judge backends:
    --judge ollama         Use local Ollama model (default)
    --judge kimi           Use Kimi K2.5 API (recommended for comparable
                           results with Mem0's published methodology)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime


def build_ollama_fn(model: str, system: str = ""):
    """Build inference function using local Ollama."""
    import requests

    def fn(prompt: str) -> str:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        resp = requests.post(
            "http://localhost:11434/api/chat",
            json={"model": model, "messages": messages, "stream": False},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()["message"]["content"]

    return fn


def build_kimi_fn(api_key: str, thinking: bool = False):
    """Build inference function using Kimi K2.5 API."""
    from openai import OpenAI

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.moonshot.ai/v1",
    )

    def fn(prompt: str) -> str:
        extra = {}
        if not thinking:
            extra["extra_body"] = {"chat_template_kwargs": {"thinking": False}}
        resp = client.chat.completions.create(
            model="kimi-k2.5",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=512,
            temperature=0.6,
            **extra,
        )
        return resp.choices[0].message.content

    return fn


def build_nvidia_nim_fn(api_key: str):
    """Build inference function using NVIDIA NIM (free tier)."""
    from openai import OpenAI

    client = OpenAI(
        api_key=api_key,
        base_url="https://integrate.api.nvidia.com/v1",
    )

    def fn(prompt: str) -> str:
        resp = client.chat.completions.create(
            model="moonshotai/kimi-k2.5",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=512,
            temperature=0.6,
        )
        return resp.choices[0].message.content

    return fn


def save_results(result, output_dir: str):
    """Save benchmark results to JSON."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"locomo_{result.mode}_{timestamp}.json"
    path = os.path.join(output_dir, filename)

    data = {
        "mode": result.mode,
        "model": result.model_name,
        "n_examples": result.n_examples,
        "overall_score": result.overall_score,
        "score_by_category": result.score_by_category,
        "n_questions": len(result.results),
        "results": [
            {
                "question": r.question,
                "answer": r.answer,
                "predicted": r.predicted,
                "category": r.category,
                "score": r.score,
                "latency_ms": r.latency_ms,
            }
            for r in result.results
        ],
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Results saved to {path}")
    return path


def main():
    parser = argparse.ArgumentParser(description="Raven LoCoMo Benchmark")
    parser.add_argument("--mode", default="both", choices=["standard", "adversarial", "both"])
    parser.add_argument("--max-examples", type=int, default=50)
    parser.add_argument(
        "--model",
        default="qwen3.5:4b",
        help="Ollama model name or 'kimi-api' or 'nvidia-nim'",
    )
    parser.add_argument("--judge", default="ollama", choices=["ollama", "kimi", "nvidia-nim"])
    parser.add_argument(
        "--n-recent",
        type=int,
        default=10,
        help="Recent nodes to inject (use 3 for adversarial pressure)",
    )
    parser.add_argument("--noise-ratio", type=float, default=0.2)
    parser.add_argument("--output", default="benchmark/results")
    parser.add_argument("--kimi-api-key", default=os.environ.get("KIMI_API_KEY"))
    parser.add_argument("--nvidia-api-key", default=os.environ.get("NVIDIA_API_KEY"))
    args = parser.parse_args()

    if args.model == "kimi-api":
        if not args.kimi_api_key:
            print("Error: --kimi-api-key or KIMI_API_KEY env required")
            sys.exit(1)
        inference_fn = build_kimi_fn(args.kimi_api_key)
        model_name = "kimi-k2.5"
    elif args.model == "nvidia-nim":
        if not args.nvidia_api_key:
            print("Error: --nvidia-api-key or NVIDIA_API_KEY env required")
            sys.exit(1)
        inference_fn = build_nvidia_nim_fn(args.nvidia_api_key)
        model_name = "kimi-k2.5-nim"
    else:
        inference_fn = build_ollama_fn(args.model)
        model_name = args.model

    if args.judge == "kimi":
        if not args.kimi_api_key:
            print("Error: --kimi-api-key required for kimi judge")
            sys.exit(1)
        judge_fn = build_kimi_fn(args.kimi_api_key)
        print("Judge: Kimi K2.5 API")
    elif args.judge == "nvidia-nim":
        if not args.nvidia_api_key:
            print("Error: --nvidia-api-key or NVIDIA_API_KEY env required for nvidia-nim judge")
            sys.exit(1)
        judge_fn = build_nvidia_nim_fn(args.nvidia_api_key)
        print("Judge: Kimi K2.5 NIM")
    else:
        judge_fn = build_ollama_fn(args.model)
        print(f"Judge: Ollama {args.model}")

    print(f"\nLoading LoCoMo dataset (max {args.max_examples} examples)...")
    from benchmark.evaluator import run_evaluation
    from benchmark.locomo_loader import load_locomo

    examples = load_locomo(split="test", max_examples=args.max_examples)
    print(f"Loaded {len(examples)} examples")

    modes = ["standard", "adversarial"] if args.mode == "both" else [args.mode]

    for mode in modes:
        print(f"\n{'=' * 60}")
        print(f"Running {mode.upper()} evaluation")
        print(f"{'=' * 60}")

        n_recent = 3 if mode == "adversarial" else args.n_recent

        with tempfile.TemporaryDirectory() as tmp_dir:
            result = run_evaluation(
                examples=examples,
                mode=mode,
                inference_fn=inference_fn,
                judge_fn=judge_fn,
                model_name=model_name,
                tmp_dir=tmp_dir,
                n_recent=n_recent,
                noise_ratio=args.noise_ratio,
                verbose=True,
            )

        print(f"\n{result.summary()}")
        save_results(result, args.output)

    if args.mode == "both":
        print(f"\n{'=' * 60}")
        print("COMPARISON vs PUBLISHED RESULTS")
        print(f"{'=' * 60}")
        print(f"{'System':<25} {'Overall':>10} {'Temporal':>10}")
        print(f"{'-' * 45}")
        print(f"{'OpenAI Memory':<25} {'52.9%':>10} {'21.7%':>10}")
        print(f"{'Zep':<25} {'66.0%':>10} {'-':>10}")
        print(f"{'Mem0':<25} {'67.1%':>10} {'58.1%':>10}")
        print(f"{'Raven (standard)':<25} {'?':>10} {'?':>10}")
        print(f"{'Raven (adversarial)':<25} {'?':>10} {'?':>10}")
        print("\n(Replace ? with your scores above)")


if __name__ == "__main__":
    main()
