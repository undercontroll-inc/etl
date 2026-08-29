from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class Stage(StrEnum):
    EXTRACT = "EXTRACT"
    RAW = "RAW"
    ENRICH = "ENRICH"
    TRANSFORM = "TRANSFORM"
    FILTER = "FILTER"
    SCORE = "SCORE"
    LOAD = "LOAD"


class Layer(StrEnum):
    ESTRUTURAL = "ESTRUTURAL"
    DOMINIO = "DOMINIO"
    INTEGRIDADE = "INTEGRIDADE"
    NEGOCIO = "NEGOCIO"
    TRANSFORM = "TRANSFORM"
    QUALIDADE_TEMPORAL = "QUALIDADE_TEMPORAL"
    API_LIMITE = "API_LIMITE"


class Severity(StrEnum):
    BLOQUEANTE = "BLOQUEANTE"
    AVISO = "AVISO"
    REJEICAO_NEGOCIO = "REJEICAO_NEGOCIO"


@dataclass
class Rejection:
    stage: Stage
    layer: Layer
    rule: str
    severity: Severity
    external_type: str | None = None
    external_id: str | None = None
    detail: str | None = None
    payload: dict[str, Any] | None = None

    def as_row(self, run_id: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "stage": str(self.stage),
            "layer": str(self.layer),
            "rule": self.rule,
            "severity": str(self.severity),
            "external_type": self.external_type,
            "external_id": self.external_id,
            "detail": self.detail,
            "payload": self.payload,
        }


@dataclass
class RejectionCollector:
    items: list[Rejection] = field(default_factory=list)

    def add(self, rejection: Rejection) -> None:
        self.items.append(rejection)

    def extend(self, rejections: list[Rejection]) -> None:
        self.items.extend(rejections)

    def rows(self, run_id: str) -> list[dict[str, Any]]:
        return [item.as_row(run_id) for item in self.items]

    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in self.items:
            key = f"{item.stage}:{item.rule}"
            counts[key] = counts.get(key, 0) + 1
        return counts

    def __len__(self) -> int:
        return len(self.items)


INVALID_EXTERNAL_TYPE = "external_type fora do dominio ITEM/PRODUCT/USER_PRODUCT"
RANK_OUT_OF_RANGE = "rank fora do intervalo 1..20"
NULL_REQUIRED_FIELD = "external_id, collected_at ou source nulo"
EMPTY_CANONICAL_TITLE = "titulo canonico vazio apos normalizacao"
OUT_OF_BUSINESS_DOMAIN = "produto fora do dominio de pequenos eletrodomesticos"
MISSING_ENRICHMENT = "enriquecimento indisponivel para o external_id"
INSUFFICIENT_HISTORY = "historico insuficiente para calcular trajetoria"
EMPTY_PAYLOAD = "payload esperado ausente na resposta da API"
