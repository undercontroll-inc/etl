from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ml_etl.domain.canonical import strip_accents

BRAND_IDS = ("BRAND", "MARCA")
MODEL_IDS = ("MODEL", "ALPHANUMERIC_MODEL", "DETAILED_MODEL", "LINE")
COLOR_IDS = ("COLOR", "MAIN_COLOR")
POWER_IDS = ("POWER", "INPUT_POWER", "NOMINAL_POWER")
VOLTAGE_IDS = ("VOLTAGE",)
CAPACITY_IDS = ("VOLUME_CAPACITY", "CAPACITY", "WEIGHT_CAPACITY")
EFFICIENCY_IDS = ("ENERGY_EFFICIENCY", "ENERGY_EFFICIENCY_RATING")

NUMBER_PATTERN = re.compile(r"\d+(?:[.,]\d+)*")
THOUSANDS_PATTERN = re.compile(r"^\d{1,3}(?:\.\d{3})+$")
VOLTAGE_PATTERN = re.compile(r"(\d{2,3})\s*v", re.IGNORECASE)
BIVOLT_PATTERN = re.compile(r"bi\s*-?\s*volt|110\s*/\s*220|220\s*/\s*110", re.IGNORECASE)
SLUG_STRIP_PATTERN = re.compile(r"[^a-z0-9]+")

CAPACITY_UNIT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("ML", re.compile(r"\b(?:ml|mililitros?)\b")),
    ("KG", re.compile(r"\b(?:kgs?|quilos?|quilogramas?)\b")),
    ("L", re.compile(r"\b(?:l|lts?|litros?)\b")),
    ("G", re.compile(r"\b(?:g|gr|gramas?)\b")),
)

MAX_SLUG_LENGTH = 120
MAX_PRODUCT_KEY_LENGTH = 180
MAX_TEXT_LENGTH = 120


@dataclass(frozen=True)
class ProductAttributes:
    brand: str | None = None
    model: str | None = None
    color: str | None = None
    voltage: str | None = None
    power_watts: int | None = None
    capacity_value: float | None = None
    capacity_unit: str | None = None
    energy_efficiency: str | None = None


def _attribute_value(attributes: Sequence[dict[str, Any]] | None, wanted: Sequence[str]) -> str | None:
    if not attributes:
        return None
    index: dict[str, str] = {}
    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue
        identifier = str(attribute.get("id") or "").upper()
        if not identifier:
            continue
        value = attribute.get("value_name")
        if not value:
            values = attribute.get("values")
            if isinstance(values, list) and values and isinstance(values[0], dict):
                value = values[0].get("name")
        if value and identifier not in index:
            index[identifier] = str(value).strip()
    for identifier in wanted:
        if index.get(identifier):
            return index[identifier]
    return None


def _to_number(raw: str | None) -> float | None:
    """Interpreta números no formato pt-BR, onde a vírgula é decimal e o ponto separa milhares."""
    if not raw:
        return None
    match = NUMBER_PATTERN.search(raw)
    if not match:
        return None

    literal = match.group(0)
    if "," in literal:
        literal = literal.replace(".", "").replace(",", ".")
    elif THOUSANDS_PATTERN.match(literal):
        literal = literal.replace(".", "")

    try:
        return float(literal)
    except ValueError:
        return None


def parse_power_watts(raw: str | None) -> int | None:
    number = _to_number(raw)
    if number is None or number <= 0 or number > 100000:
        return None
    return int(round(number))


def parse_voltage(raw: str | None) -> str | None:
    if not raw:
        return None
    if BIVOLT_PATTERN.search(raw):
        return "BIVOLT"
    match = VOLTAGE_PATTERN.search(raw)
    if match:
        return f"{int(match.group(1))}V"
    number = _to_number(raw)
    if number is None:
        return None
    return f"{int(number)}V"


def parse_capacity(raw: str | None) -> tuple[float | None, str | None]:
    if not raw:
        return None, None
    number = _to_number(raw)
    if number is None or number <= 0:
        return None, None

    without_number = NUMBER_PATTERN.sub(" ", strip_accents(raw.lower()))
    for unit, pattern in CAPACITY_UNIT_PATTERNS:
        if pattern.search(without_number):
            return round(number, 3), unit
    return round(number, 3), None


def slugify(value: str | None, max_length: int = MAX_SLUG_LENGTH) -> str | None:
    if not value:
        return None
    normalized = strip_accents(str(value).lower())
    slug = SLUG_STRIP_PATTERN.sub("-", normalized).strip("-")
    return slug[:max_length] or None


def extract_attributes(
    attributes: Sequence[dict[str, Any]] | None,
    fallback_brand: str | None = None,
) -> ProductAttributes:
    capacity_value, capacity_unit = parse_capacity(_attribute_value(attributes, CAPACITY_IDS))
    brand = _attribute_value(attributes, BRAND_IDS) or fallback_brand
    return ProductAttributes(
        brand=str(brand)[:MAX_TEXT_LENGTH] if brand else None,
        model=(_attribute_value(attributes, MODEL_IDS) or None),
        color=(_attribute_value(attributes, COLOR_IDS) or None),
        voltage=parse_voltage(_attribute_value(attributes, VOLTAGE_IDS)),
        power_watts=parse_power_watts(_attribute_value(attributes, POWER_IDS)),
        capacity_value=capacity_value,
        capacity_unit=capacity_unit,
        energy_efficiency=(_attribute_value(attributes, EFFICIENCY_IDS) or None),
    )


def build_product_key(brand: str | None, model: str | None, domain_id: str | None) -> str | None:
    brand_slug = slugify(brand)
    model_slug = slugify(model)
    domain_slug = slugify(domain_id)

    if brand_slug and model_slug:
        return f"{brand_slug}:{model_slug}"[:MAX_PRODUCT_KEY_LENGTH]
    if brand_slug and domain_slug:
        return f"{brand_slug}:{domain_slug}"[:MAX_PRODUCT_KEY_LENGTH]
    return None
