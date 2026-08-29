from __future__ import annotations

import re
import unicodedata

STOPWORDS = frozenset(
    {
        "a",
        "as",
        "ao",
        "aos",
        "com",
        "cor",
        "da",
        "das",
        "de",
        "do",
        "dos",
        "e",
        "em",
        "envio",
        "frete",
        "gratis",
        "na",
        "no",
        "novo",
        "o",
        "os",
        "original",
        "para",
        "por",
        "pra",
        "promocao",
        "um",
        "uma",
        "unidade",
        "com nf",
        "nf",
    }
)

NUMBER = r"(\d+(?:\.\d+)?)"

UNIT_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(rf"{NUMBER}[\s]*(?:litros|litro|lts|lt|l)\b"), r"\1l"),
    (re.compile(rf"{NUMBER}[\s]*(?:mililitros|ml)\b"), r"\1ml"),
    (re.compile(rf"{NUMBER}[\s]*(?:watts|watt|w)\b"), r"\1w"),
    (re.compile(rf"{NUMBER}[\s]*(?:volts|volt|v)\b"), r"\1v"),
    (re.compile(rf"{NUMBER}[\s]*(?:quilos|quilo|kgs|kg)\b"), r"\1kg"),
    (re.compile(rf"{NUMBER}[\s]*(?:gramas|grama|gr|g)\b"), r"\1g"),
    (re.compile(rf"{NUMBER}[\s]*(?:polegadas|polegada|pol)\b"), r"\1in"),
)

PUNCTUATION_PATTERN = re.compile(r"[^a-z0-9.\s]+")
ORPHAN_DOT_PATTERN = re.compile(r"(?<!\d)\.|\.(?!\d)")
WHITESPACE_PATTERN = re.compile(r"\s+")
DECIMAL_COMMA_PATTERN = re.compile(r"(\d),(\d)")


def strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def normalize_units(value: str) -> str:
    normalized = DECIMAL_COMMA_PATTERN.sub(r"\1.\2", value)
    for pattern, replacement in UNIT_PATTERNS:
        normalized = pattern.sub(replacement, normalized)
    return normalized


def tokenize(value: str) -> list[str]:
    return [token for token in WHITESPACE_PATTERN.split(value) if token]


def remove_stopwords(tokens: list[str]) -> list[str]:
    filtered = [token for token in tokens if token not in STOPWORDS and len(token) > 1]
    return filtered or tokens


def canonicalize(title: str | None) -> str:
    if not title:
        return ""
    lowered = title.lower()
    without_accents = strip_accents(lowered)
    with_units = normalize_units(without_accents)
    without_punctuation = PUNCTUATION_PATTERN.sub(" ", with_units)
    without_orphan_dots = ORPHAN_DOT_PATTERN.sub(" ", without_punctuation)
    collapsed = WHITESPACE_PATTERN.sub(" ", without_orphan_dots).strip()
    tokens = remove_stopwords(tokenize(collapsed))
    return " ".join(tokens)


def canonical_tokens(title: str | None) -> set[str]:
    return set(canonicalize(title).split())
