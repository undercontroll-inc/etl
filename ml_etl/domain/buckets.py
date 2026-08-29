from __future__ import annotations

from datetime import datetime, timedelta, timezone

RANK_BANDS = (
    (1, 5, "MUITO_ALTO"),
    (6, 10, "ALTO"),
    (11, 20, "MODERADO"),
)
STRONG_GROWTH_THRESHOLD = 10


def bucket_key_for(moment: datetime, granularity: str = "month") -> str:
    reference = moment.astimezone(timezone.utc) if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
    if granularity == "day":
        return reference.strftime("%Y-%m-%d")
    return reference.strftime("%Y-%m")


def previous_bucket_key(bucket: str, granularity: str = "month") -> str:
    if granularity == "day":
        current = datetime.strptime(bucket, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (current - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month = (int(part) for part in bucket.split("-"))
    if month == 1:
        return f"{year - 1}-12"
    return f"{year}-{month - 1:02d}"


def rank_band(rank: int | None) -> str:
    if rank is None:
        return "INDEFINIDO"
    for lower, upper, label in RANK_BANDS:
        if lower <= rank <= upper:
            return label
    return "INDEFINIDO"


def trajectory_label(rank_delta: int | None) -> str:
    if rank_delta is None:
        return "SEM_HISTORICO"
    if rank_delta >= STRONG_GROWTH_THRESHOLD:
        return "CRESCIMENTO_FORTE"
    if rank_delta > 0:
        return "CRESCIMENTO"
    if rank_delta == 0:
        return "ESTAVEL"
    return "QUEDA"


def is_valid_bucket(bucket: str, granularity: str = "month") -> bool:
    fmt = "%Y-%m-%d" if granularity == "day" else "%Y-%m"
    try:
        datetime.strptime(bucket, fmt)
    except ValueError:
        return False
    return True
