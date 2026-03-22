from __future__ import annotations

import random


NOISE_EVENTS = [
    "turned on the kettle",
    "checked the weather forecast",
    "adjusted monitor brightness",
    "opened a new browser tab",
    "refilled water bottle",
    "checked notifications on phone",
]


def should_crash_session(crash_rate: float = 0.3) -> bool:
    """Return whether a session should terminate without reconciliation."""
    return random.random() < crash_rate


def should_inject_noise(noise_ratio: float = 0.2) -> bool:
    """Return whether this session should receive injected noise nodes."""
    return random.random() < noise_ratio


def sample_noise_event() -> str:
    """Return one irrelevant filler event used for robustness testing."""
    return random.choice(NOISE_EVENTS)
