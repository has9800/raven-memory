from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LoCoMoTurn:
    session_idx: int
    turn_idx: int
    speaker: str  # "human" or "assistant"
    text: str
    timestamp: str  # synthetic ISO date from LoCoMo


@dataclass
class LoCoMoQA:
    question: str
    answer: str
    category: str  # single_hop, multi_hop, temporal, summarization
    evidence_sessions: list[int]  # which sessions contain the answer


@dataclass
class LoCoMoExample:
    example_id: str
    persona_a: str
    persona_b: str
    turns: list[LoCoMoTurn]
    questions: list[LoCoMoQA]
    n_sessions: int


def load_locomo(split: str = "test", max_examples: int = 50) -> list[LoCoMoExample]:
    """
    Load LoCoMo dataset from HuggingFace.
    Returns parsed LoCoMoExample objects.

    Args:
        split: "train" or "test"
        max_examples: limit for faster evaluation during development
    """
    from datasets import load_dataset

    ds = load_dataset("Snapchat-research/locomo", split=split)
    examples: list[LoCoMoExample] = []

    for i, item in enumerate(ds):
        if i >= max_examples:
            break

        turns: list[LoCoMoTurn] = []
        for sess_idx, session in enumerate(item["conversation"]):
            for turn_idx, turn in enumerate(session):
                turns.append(
                    LoCoMoTurn(
                        session_idx=sess_idx,
                        turn_idx=turn_idx,
                        speaker=turn.get("speaker", "human"),
                        text=turn.get("text", ""),
                        timestamp=turn.get("timestamp", ""),
                    )
                )

        questions: list[LoCoMoQA] = []
        for qa in item.get("qa", []):
            questions.append(
                LoCoMoQA(
                    question=qa.get("question", ""),
                    answer=qa.get("answer", ""),
                    category=qa.get("category", "single_hop"),
                    evidence_sessions=qa.get("evidence_sessions", []),
                )
            )

        examples.append(
            LoCoMoExample(
                example_id=str(i),
                persona_a=item.get("persona_a", ""),
                persona_b=item.get("persona_b", ""),
                turns=turns,
                questions=questions,
                n_sessions=len(item["conversation"]),
            )
        )

    return examples
