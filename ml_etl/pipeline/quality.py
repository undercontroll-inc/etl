from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import pandas as pd

from ml_etl.config import QualitySettings
from ml_etl.domain.rejection import Rejection, Severity
from ml_etl.pipeline.extract import SOURCE_HIGHLIGHTS, RawPayload


class Verdict(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


RUN_STATUS_BY_VERDICT = {
    Verdict.PASS: "SUCCESS",
    Verdict.WARN: "PARTIAL",
    Verdict.FAIL: "FAILED",
}


@dataclass(frozen=True)
class Check:
    name: str
    value: float
    verdict: Verdict
    warn_below: float | None = None
    fail_below: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "verdict": str(self.verdict),
            "warn_below": self.warn_below,
            "fail_below": self.fail_below,
        }


@dataclass(frozen=True)
class QualityReport:
    verdict: Verdict
    checks: list[Check] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def run_status(self) -> str:
        return RUN_STATUS_BY_VERDICT[self.verdict]

    def failures(self) -> list[str]:
        return [check.name for check in self.checks if check.verdict == Verdict.FAIL]

    def as_dict(self) -> dict[str, Any]:
        return {
            "verdict": str(self.verdict),
            "checks": [check.as_dict() for check in self.checks],
            "metrics": self.metrics,
        }


def grade(name: str, value: float, warn_below: float | None = None, fail_below: float | None = None) -> Check:
    if fail_below is not None and value < fail_below:
        verdict = Verdict.FAIL
    elif warn_below is not None and value < warn_below:
        verdict = Verdict.WARN
    else:
        verdict = Verdict.PASS
    return Check(name=name, value=round(float(value), 4), verdict=verdict, warn_below=warn_below, fail_below=fail_below)


def evaluate(
    target_categories: Sequence[str],
    payloads: Sequence[RawPayload],
    curated: pd.DataFrame,
    rejections: Sequence[Rejection],
    settings: QualitySettings,
) -> QualityReport:
    targets = set(target_categories)
    covered = {
        payload.request_params.get("category_id") for payload in payloads if payload.source == SOURCE_HIGHLIGHTS
    } & targets
    coverage = len(covered) / len(targets) if targets else 0.0

    curated_count = len(curated)
    priced = 0
    if curated_count and "price_median" in curated.columns:
        priced = int(curated["price_median"].notna().sum())
    price_coverage = priced / curated_count if curated_count else 0.0

    checks = [
        grade("category_coverage", coverage, warn_below=1.0, fail_below=settings.min_category_coverage),
        grade(
            "curated_products",
            curated_count,
            warn_below=settings.warn_curated_products,
            fail_below=settings.min_curated_products,
        ),
        grade("price_coverage", price_coverage, warn_below=settings.min_price_coverage),
    ]
    verdicts = {check.verdict for check in checks}
    if Verdict.FAIL in verdicts:
        verdict = Verdict.FAIL
    elif Verdict.WARN in verdicts:
        verdict = Verdict.WARN
    else:
        verdict = Verdict.PASS

    metrics = {
        "target_categories": sorted(targets),
        "covered_categories": sorted(covered),
        "curated_products": curated_count,
        "priced_products": priced,
        "rejections": len(rejections),
        "blocking_rejections": sum(1 for item in rejections if item.severity == Severity.BLOQUEANTE),
    }
    return QualityReport(verdict=verdict, checks=checks, metrics=metrics)
