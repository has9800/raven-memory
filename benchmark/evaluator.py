from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Callable

from tcc.core.dag import TaskDAG
from tcc.core.reconciler import SessionReconciler


@dataclass
class EvalResult:
    question: str
    answer: str
    predicted: str
    category: str
    score: float  # 0.0 or 1.0 from LLM judge
    latency_ms: float
    tokens_used: int = 0


@dataclass
class BenchmarkResult:
    mode: str
    model_name: str
    n_examples: int
    results: list[EvalResult] = field(default_factory=list)

    @property
    def overall_score(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.score for r in self.results) / len(self.results)

    @property
    def score_by_category(self) -> dict[str, float]:
        categories: dict[str, list[float]] = {}
        for r in self.results:
            categories.setdefault(r.category, []).append(r.score)
        return {k: sum(v) / len(v) for k, v in categories.items()}

    def summary(self) -> str:
        lines = [
            f"Mode: {self.mode}",
            f"Model: {self.model_name}",
            f"Examples: {self.n_examples}",
            f"Overall: {self.overall_score:.1%}",
            "",
            "By category:",
        ]
        for cat, score in sorted(self.score_by_category.items()):
            lines.append(f"  {cat}: {score:.1%}")
        return "\n".join(lines)


def answer_question(
    question: str,
    dag: TaskDAG,
    reconciler: SessionReconciler,
    inference_fn: Callable[[str], str],
    n_recent: int = 10,
) -> tuple[str, float, int]:
    """
    Use Raven to answer a LoCoMo question.

    Returns (answer, latency_ms, tokens_used)
    """
    start = time.time()

    ctx = reconciler.start_session(
        dag,
        n_recent=n_recent,
        search_query=question,
        n_search=5,
    )

    prompt = f"""You are answering questions about a long conversation history.
Use the context below to answer the question accurately and concisely.

{ctx['summary']}

Question: {question}

Answer in 1-3 sentences. Be specific and factual."""

    answer = inference_fn(prompt) or ""
    latency_ms = (time.time() - start) * 1000

    return answer, latency_ms, 0


def judge_answer(
    question: str,
    ground_truth: str,
    predicted: str,
    judge_fn: Callable[[str], str],
) -> float:
    """
    Use LLM-as-a-Judge to score the predicted answer.
    Returns 0.0 (wrong) or 1.0 (correct).
    Follows Mem0's published evaluation protocol for comparability.
    """
    prompt = f"""You are evaluating an AI assistant's answer to a question
about a conversation.

Question: {question}
Ground truth answer: {ground_truth}
AI assistant's answer: {predicted}

Does the AI assistant's answer correctly capture the key information
from the ground truth?

Respond with ONLY one of:
CORRECT
INCORRECT

Do not explain. Just CORRECT or INCORRECT."""

    raw = judge_fn(prompt)
    if not raw:
        return 0.0
    judgment = raw.strip().upper()
    if "CORRECT" in judgment and "INCORRECT" not in judgment:
        return 1.0
    return 0.0


def run_evaluation(
    examples,
    mode: str,
    inference_fn: Callable[[str], str],
    judge_fn: Callable[[str], str],
    model_name: str,
    tmp_dir: str,
    n_recent: int = 10,
    noise_ratio: float = 0.2,
    verbose: bool = True,
) -> BenchmarkResult:
    """
    Run full evaluation over all examples.

    Args:
        examples: list of LoCoMoExample
        mode: "standard" or "adversarial"
        inference_fn: callable(prompt: str) -> str
        judge_fn: callable(prompt: str) -> str
        model_name: name for reporting
        tmp_dir: directory for per-example databases
        n_recent: nodes to inject at session start
        noise_ratio: noise injection rate (adversarial only)
        verbose: print progress
    """
    from .raven_ingester import create_example_db, ingest_example

    random.seed(42)

    result = BenchmarkResult(
        mode=mode,
        model_name=model_name,
        n_examples=len(examples),
    )

    for i, example in enumerate(examples):
        if verbose:
            print(
                f"  Example {i + 1}/{len(examples)} "
                f"(id={example.example_id}, "
                f"sessions={example.n_sessions}, "
                f"questions={len(example.questions)})"
            )

        db_path = create_example_db(example.example_id, tmp_dir)

        dag, reconciler, _store = ingest_example(
            example,
            db_path=db_path,
            mode=mode,
            noise_ratio=noise_ratio,
            n_recent_inject=n_recent,
        )

        for qa in example.questions:
            try:
                predicted, latency_ms, tokens = answer_question(
                    qa.question,
                    dag,
                    reconciler,
                    inference_fn,
                    n_recent=n_recent,
                )
                score = judge_answer(
                    qa.question,
                    qa.answer,
                    predicted,
                    judge_fn,
                )
                result.results.append(
                    EvalResult(
                        question=qa.question,
                        answer=qa.answer,
                        predicted=predicted,
                        category=qa.category,
                        score=score,
                        latency_ms=latency_ms,
                        tokens_used=tokens,
                    )
                )
            except Exception as e:
                if verbose:
                    print(f"    [ERROR] {qa.question[:60]}: {e}")
                result.results.append(
                    EvalResult(
                        question=qa.question,
                        answer=qa.answer,
                        predicted="",
                        category=qa.category,
                        score=0.0,
                        latency_ms=0.0,
                    )
                )

            # Print running score every 10 questions
            if verbose and len(result.results) % 10 == 0 and result.results:
                n = len(result.results)
                correct = sum(r.score for r in result.results)
                print(f"    [{n} questions] running score: "
                      f"{correct/n:.1%}")

        try:
            import os

            os.remove(db_path)
        except Exception:
            pass

    return result
