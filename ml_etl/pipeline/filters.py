from __future__ import annotations

import pandas as pd

from ml_etl.domain.rejection import OUT_OF_BUSINESS_DOMAIN, Layer, Rejection, Severity, Stage
from ml_etl.domain.whitelist import classify_small_appliance
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)


def filter_small_appliances(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[Rejection]]:
    rejections: list[Rejection] = []
    if frame.empty:
        working = frame.copy()
        working["relevance"] = []
        working["domain_reason"] = []
        return working, rejections

    working = frame.copy()
    classifications = [
        classify_small_appliance(
            row["title"],
            row.get("enriched_category_id") or row.get("category_id"),
            row.get("domain_id"),
        )
        for _, row in working.iterrows()
    ]

    working["relevance"] = [item.relevance for item in classifications]
    working["domain_reason"] = [item.reason for item in classifications]
    in_domain = pd.Series([item.in_domain for item in classifications], index=working.index)

    for position, classification in enumerate(classifications):
        if classification.in_domain:
            continue
        row = working.iloc[position]
        rejections.append(
            Rejection(
                stage=Stage.FILTER,
                layer=Layer.NEGOCIO,
                rule=OUT_OF_BUSINESS_DOMAIN,
                severity=Severity.REJEICAO_NEGOCIO,
                external_type=row["external_type"],
                external_id=row["external_id"],
                detail=f"motivo: {classification.reason}; titulo: {row['title']!r}",
            )
        )

    kept = working[in_domain].reset_index(drop=True)
    logger.info("FILTER: %s registros no dominio, %s descartados", len(kept), len(rejections))
    return kept, rejections
