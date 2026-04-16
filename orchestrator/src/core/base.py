"""Cost accounting for LLM/agent invocations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List


@dataclass
class InvocationCost:
    label: str
    children: List[InvocationCost] = field(default_factory=list)
    total_tokens: int = 0


def extract_runner_cost(result: Any, label: str) -> InvocationCost:
    total = 0
    for resp in getattr(result, "raw_responses", None) or []:
        usage = getattr(resp, "usage", None)
        if usage is None:
            continue
        t = getattr(usage, "total_tokens", None)
        if t is not None:
            total += int(t)
            continue
        inp = getattr(usage, "input_tokens", None)
        out = getattr(usage, "output_tokens", None)
        if inp is not None or out is not None:
            total += int(inp or 0) + int(out or 0)
    return InvocationCost(label=label, children=[], total_tokens=total)
