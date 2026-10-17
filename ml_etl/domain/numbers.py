from __future__ import annotations

from typing import Any


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_positive_float(value: Any) -> float | None:
    result = as_float(value)
    return result if result is not None and result > 0 else None


def as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
