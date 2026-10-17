from __future__ import annotations

from dataclasses import dataclass

from ml_etl.domain.canonical import canonicalize

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
        "MLB73057",
        "MLB456045",
    }
)

DOMAIN_WHITELIST = frozenset(
    {
        "MLB-AIR_FRYERS",
        "MLB-BLENDERS",
        "MLB-BREAD_MAKERS",
        "MLB-COFFEE_MAKERS",
        "MLB-COOKTOPS",
        "MLB-DEEP_FRYERS",
        "MLB-ELECTRIC_BARBECUE_GRILLS",
        "MLB-ELECTRIC_GRILLS",
        "MLB-ELECTRIC_JUGS",
        "MLB-ELECTRIC_KETTLES",
        "MLB-ELECTRIC_OVENS",
        "MLB-ELECTRIC_PRESSURE_COOKERS",
        "MLB-ELECTRIC_SANDWICH_MAKERS",
        "MLB-ELECTRIC_SQUEEZERS",
        "MLB-ESPRESSO_MACHINES",
        "MLB-FANS",
        "MLB-FOOD_PROCESSORS",
        "MLB-FOOD_STEAMERS",
        "MLB-HAIR_DRYERS",
        "MLB-HAIR_STRAIGHTENERS",
        "MLB-HAND_BLENDERS",
        "MLB-HUMIDIFIERS",
        "MLB-ICE_CREAM_MAKERS",
        "MLB-IRONS",
        "MLB-MICROWAVES",
        "MLB-MIXERS",
        "MLB-POPCORN_MACHINES",
        "MLB-STAND_MIXERS",
        "MLB-TOASTERS",
        "MLB-VACUUM_AND_STEAM_CLEANERS",
        "MLB-VACUUM_CLEANERS",
        "MLB-WAFFLE_MAKERS",
        "MLB-WATER_PURIFIERS",
        "MLB-YOGURT_MAKERS",
    }
)

DOMAIN_BLACKLIST = frozenset(
    {
        "MLB-AIR_FRYER_ACCESSORIES",
        "MLB-APPLIANCE_PARTS",
        "MLB-BATTERIES",
        "MLB-LIGHT_BULBS",
        "MLB-POWER_ADAPTERS",
        "MLB-VACUUM_CLEANER_BAGS_AND_FILTERS",
        "MLB-WATER_PURIFIER_FILTERS_REPLACEMENTS",
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

RELEVANCE_BASE = {
    "DOMINIO_WHITELIST": 0.75,
    "CATEGORIA_WHITELIST": 0.6,
    "TERMO_WHITELIST": 0.5,
}


@dataclass(frozen=True)
class Classification:
    in_domain: bool
    reason: str
    matched_terms: tuple[str, ...] = ()

    @property
    def relevance(self) -> float:
        if not self.in_domain:
            return 0.0
        base = RELEVANCE_BASE.get(self.reason, 0.5)
        bonus = min(0.25, 0.1 * len(self.matched_terms))
        return round(min(1.0, base + bonus), 4)


def is_whitelisted_category(category_id: str | None) -> bool:
    return bool(category_id) and category_id in CATEGORY_WHITELIST


def is_whitelisted_domain(domain_id: str | None) -> bool:
    return bool(domain_id) and domain_id in DOMAIN_WHITELIST


def _first_index(tokens: list[str], vocabulary: frozenset[str]) -> int | None:
    for index, token in enumerate(tokens):
        if token in vocabulary:
            return index
    return None


def classify_small_appliance(
    title: str | None,
    category_id: str | None = None,
    domain_id: str | None = None,
) -> Classification:
    """Decide na ordem: blacklist de dominio, blacklist de termo, whitelist de dominio,
    whitelist de termo e por fim whitelist de categoria."""
    ordered = canonicalize(title).split()
    tokens = set(ordered)

    if domain_id and domain_id in DOMAIN_BLACKLIST:
        return Classification(in_domain=False, reason="DOMINIO_BLACKLIST")

    if not tokens:
        return Classification(in_domain=False, reason="TITULO_VAZIO")

    if len(tokens) < MIN_TOKENS:
        return Classification(in_domain=False, reason="TITULO_INSUFICIENTE")

    blocked = tuple(sorted(tokens & TERM_BLACKLIST))
    matched = tuple(sorted(tokens & TERM_WHITELIST))

    blocked_at = _first_index(ordered, TERM_BLACKLIST)
    appliance_at = _first_index(ordered, TERM_WHITELIST)

    # Anuncio de acessorio abre o titulo com o acessorio ("Capa Para Air Fryer"); aparelho que
    # apenas menciona um acessorio abre com o aparelho ("Aspirador ... Filtro Lavavel").
    if blocked_at is not None and (appliance_at is None or blocked_at < appliance_at):
        return Classification(in_domain=False, reason="TERMO_BLACKLIST", matched_terms=blocked)

    if is_whitelisted_domain(domain_id):
        return Classification(in_domain=True, reason="DOMINIO_WHITELIST", matched_terms=matched)

    if matched:
        return Classification(in_domain=True, reason="TERMO_WHITELIST", matched_terms=matched)

    if is_whitelisted_category(category_id):
        return Classification(in_domain=True, reason="CATEGORIA_WHITELIST")

    return Classification(in_domain=False, reason="FORA_DO_DOMINIO")
