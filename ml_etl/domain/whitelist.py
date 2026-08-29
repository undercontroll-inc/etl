from __future__ import annotations

from dataclasses import dataclass

from ml_etl.domain.canonical import canonical_tokens

ROOT_CATEGORY = "MLB5726"

CATEGORY_WHITELIST = frozenset(
    {
        "MLB5726",
        "MLB1574",
        "MLB271599",
        "MLB271600",
        "MLB432825",
        "MLB438563",
        "MLB6157",
        "MLB6212",
        "MLB6213",
        "MLB6215",
        "MLB6216",
        "MLB6217",
        "MLB6219",
        "MLB6220",
        "MLB6224",
        "MLB6229",
        "MLB6231",
        "MLB6234",
        "MLB6236",
        "MLB6238",
        "MLB6240",
    }
)

TERM_WHITELIST = frozenset(
    {
        "airfryer",
        "fryer",
        "fritadeira",
        "cafeteira",
        "expresso",
        "liquidificador",
        "batedeira",
        "mixer",
        "processador",
        "espremedor",
        "sanduicheira",
        "grill",
        "torradeira",
        "chaleira",
        "eletrica",
        "eletrico",
        "panela",
        "multiprocessador",
        "microondas",
        "forno",
        "cooktop",
        "ventilador",
        "circulador",
        "climatizador",
        "purificador",
        "umidificador",
        "aspirador",
        "robo",
        "vaporizador",
        "ferro",
        "secador",
        "chapinha",
        "prancha",
        "barbeador",
        "aparador",
        "depilador",
        "escova",
        "espiralizador",
        "iogurteira",
        "pipoqueira",
        "crepeira",
        "waffle",
        "omeleteira",
        "fondue",
        "churrasqueira",
        "adega",
        "frigobar",
        "desumidificador",
        "lavadora",
        "extratora",
        "espumador",
        "moedor",
        "fatiadora",
        "cortador",
        "balanca",
        "termopote",
        "garrafa",
        "sorveteira",
        "maquina",
        "pao",
        "fogareiro",
    }
)

TERM_BLACKLIST = frozenset(
    {
        "capa",
        "resistencia",
        "filtro",
        "refil",
        "carvao",
        "cabo",
        "adaptador",
        "suporte",
        "peca",
        "reposicao",
        "escovinha",
        "manual",
        "adesivo",
        "protetor",
        "papel",
        "sache",
        "capsula",
        "capsulas",
        "detergente",
        "sabao",
        "amaciante",
        "lampada",
        "pilha",
        "bateria",
        "carregador",
        "controle",
        "remoto",
        "camiseta",
        "livro",
        "curso",
        "servico",
        "instalacao",
        "seguro",
        "garantia",
    }
)

MIN_TOKENS = 2


@dataclass(frozen=True)
class Classification:
    in_domain: bool
    reason: str
    matched_terms: tuple[str, ...] = ()

    @property
    def relevance(self) -> float:
        if not self.in_domain:
            return 0.0
        base = 0.6 if self.reason == "CATEGORIA_WHITELIST" else 0.5
        bonus = min(0.4, 0.15 * len(self.matched_terms))
        return round(min(1.0, base + bonus), 4)


def is_whitelisted_category(category_id: str | None) -> bool:
    return bool(category_id) and category_id in CATEGORY_WHITELIST


def classify_small_appliance(title: str | None, category_id: str | None = None) -> Classification:
    tokens = canonical_tokens(title)

    if not tokens:
        return Classification(in_domain=False, reason="TITULO_VAZIO")

    if len(tokens) < MIN_TOKENS:
        return Classification(in_domain=False, reason="TITULO_INSUFICIENTE")

    blocked = tuple(sorted(tokens & TERM_BLACKLIST))
    matched = tuple(sorted(tokens & TERM_WHITELIST))

    if blocked:
        return Classification(in_domain=False, reason="TERMO_BLACKLIST", matched_terms=blocked)

    if matched:
        return Classification(in_domain=True, reason="TERMO_WHITELIST", matched_terms=matched)

    if is_whitelisted_category(category_id):
        return Classification(in_domain=True, reason="CATEGORIA_WHITELIST")

    return Classification(in_domain=False, reason="FORA_DO_DOMINIO")
