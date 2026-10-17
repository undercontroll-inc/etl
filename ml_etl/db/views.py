from __future__ import annotations

from sqlalchemy import Engine, text

from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

RISING_TRAJECTORIES = "('CRESCIMENTO', 'CRESCIMENTO_FORTE')"

VW_PRODUCT_CURRENT = """
CREATE VIEW vw_market_product_current AS
SELECT
    c.bucket_key,
    c.source,
    c.external_type,
    c.external_id,
    c.title,
    c.canonical_title,
    c.rank,
    c.rank_band,
    c.rank_delta,
    c.trajectory_label,
    c.score,
    c.priority_label,
    c.confidence,
    c.domain_id,
    c.category_id,
    c.category_name,
    c.brand,
    c.brand_slug,
    c.model,
    c.product_key,
    c.voltage,
    c.power_watts,
    c.capacity_value,
    c.capacity_unit,
    c.energy_efficiency,
    c.price,
    c.price_min,
    c.price_median,
    c.price_max,
    c.price_delta_pct,
    c.discount_pct,
    c.offer_count,
    c.seller_count,
    c.relevance,
    c.domain_reason,
    c.permalink,
    c.collected_at
FROM market_curated_product c
WHERE c.bucket_key = (SELECT MAX(bucket_key) FROM market_curated_product)
"""

VW_RISING_PRODUCTS = f"""
CREATE VIEW vw_market_rising_products AS
SELECT
    bucket_key,
    external_type,
    external_id,
    title,
    brand,
    brand_slug,
    model,
    product_key,
    domain_id,
    category_id,
    category_name,
    rank,
    rank_band,
    rank_delta,
    trajectory_label,
    score,
    priority_label,
    confidence,
    price_median,
    price_min,
    price_max,
    price_delta_pct,
    discount_pct,
    offer_count,
    seller_count,
    capacity_value,
    capacity_unit,
    power_watts,
    voltage,
    permalink
FROM vw_market_product_current
WHERE trajectory_label IN {RISING_TRAJECTORIES}
   OR priority_label = 'PRIORIDADE_ALTA'
"""

VW_CATEGORY_SUMMARY = """
CREATE VIEW vw_market_category_summary AS
SELECT
    domain_id,
    bucket_key,
    MAX(category_name) AS category_name_sample,
    COUNT(*) AS product_count,
    COUNT(DISTINCT brand_slug) AS brand_count,
    ROUND(CAST(AVG(score) AS NUMERIC), 2) AS avg_score,
    MAX(score) AS max_score,
    ROUND(CAST(AVG(price_median) AS NUMERIC), 2) AS avg_price_median,
    MIN(price_min) AS price_floor,
    MAX(price_max) AS price_ceiling,
    ROUND(CAST(AVG(offer_count) AS NUMERIC), 2) AS avg_offer_count,
    ROUND(CAST(AVG(price_delta_pct) AS NUMERIC), 2) AS avg_price_delta_pct,
    ROUND(CAST(AVG(discount_pct) AS NUMERIC), 2) AS avg_discount_pct,
    SUM(CASE WHEN trajectory_label IN ('CRESCIMENTO', 'CRESCIMENTO_FORTE') THEN 1 ELSE 0 END) AS rising_count,
    SUM(CASE WHEN trajectory_label = 'QUEDA' THEN 1 ELSE 0 END) AS falling_count,
    SUM(CASE WHEN confidence = 'HIGH' THEN 1 ELSE 0 END) AS high_confidence_count
FROM market_curated_product
WHERE domain_id IS NOT NULL
GROUP BY domain_id, bucket_key
"""

VW_BRAND_SUMMARY = """
CREATE VIEW vw_market_brand_summary AS
SELECT
    brand_slug,
    bucket_key,
    MAX(brand) AS brand_name,
    COUNT(*) AS product_count,
    COUNT(DISTINCT domain_id) AS domain_count,
    ROUND(CAST(AVG(score) AS NUMERIC), 2) AS avg_score,
    MIN(rank) AS best_rank,
    ROUND(CAST(AVG(price_median) AS NUMERIC), 2) AS avg_price_median,
    MIN(price_min) AS price_floor,
    MAX(price_max) AS price_ceiling,
    ROUND(CAST(AVG(price_delta_pct) AS NUMERIC), 2) AS avg_price_delta_pct,
    ROUND(CAST(AVG(discount_pct) AS NUMERIC), 2) AS avg_discount_pct,
    SUM(CASE WHEN trajectory_label IN ('CRESCIMENTO', 'CRESCIMENTO_FORTE') THEN 1 ELSE 0 END) AS rising_count
FROM market_curated_product
WHERE brand_slug IS NOT NULL
GROUP BY brand_slug, bucket_key
"""

VW_PRICE_MOVEMENT = """
CREATE VIEW vw_market_price_movement AS
WITH domain_bucket AS (
    SELECT
        e.domain_id AS domain_id,
        p.bucket_key AS bucket_key,
        COUNT(*) AS product_count,
        SUM(p.offer_count) AS offer_count,
        SUM(p.seller_count) AS seller_count,
        AVG(p.price_median) AS avg_price_median
    FROM market_price_observation p
    JOIN market_entity e
      ON e.external_type = p.external_type
     AND e.external_id = p.external_id
    WHERE e.domain_id IS NOT NULL
      AND p.price_median IS NOT NULL
    GROUP BY e.domain_id, p.bucket_key
),
with_previous AS (
    SELECT
        domain_id,
        bucket_key,
        product_count,
        offer_count,
        seller_count,
        avg_price_median,
        LAG(avg_price_median) OVER (PARTITION BY domain_id ORDER BY bucket_key) AS previous_price_median,
        LAG(bucket_key) OVER (PARTITION BY domain_id ORDER BY bucket_key) AS previous_bucket_key,
        LAG(offer_count) OVER (PARTITION BY domain_id ORDER BY bucket_key) AS previous_offer_count
    FROM domain_bucket
)
SELECT
    domain_id,
    bucket_key,
    previous_bucket_key,
    product_count,
    offer_count,
    seller_count,
    ROUND(CAST(avg_price_median AS NUMERIC), 2) AS avg_price_median,
    ROUND(CAST(previous_price_median AS NUMERIC), 2) AS previous_price_median,
    CASE
        WHEN previous_price_median IS NULL OR previous_price_median <= 0 THEN NULL
        ELSE ROUND(CAST((avg_price_median - previous_price_median) / previous_price_median * 100 AS NUMERIC), 2)
    END AS price_delta_pct,
    CASE
        WHEN previous_offer_count IS NULL OR previous_offer_count <= 0 THEN NULL
        ELSE ROUND(CAST((offer_count - previous_offer_count) * 100.0 / previous_offer_count AS NUMERIC), 2)
    END AS offer_delta_pct
FROM with_previous
"""

VIEWS: tuple[tuple[str, str], ...] = (
    ("vw_market_product_current", VW_PRODUCT_CURRENT),
    ("vw_market_rising_products", VW_RISING_PRODUCTS),
    ("vw_market_category_summary", VW_CATEGORY_SUMMARY),
    ("vw_market_brand_summary", VW_BRAND_SUMMARY),
    ("vw_market_price_movement", VW_PRICE_MOVEMENT),
)


def create_views(engine: Engine) -> list[str]:
    created: list[str] = []
    with engine.begin() as connection:
        for name, _ in reversed(VIEWS):
            connection.execute(text(f"DROP VIEW IF EXISTS {name}"))
        for name, statement in VIEWS:
            connection.execute(text(statement))
            created.append(name)
    logger.info("Views de consumo criadas: %s", ", ".join(created))
    return created


def drop_views(engine: Engine) -> None:
    with engine.begin() as connection:
        for name, _ in reversed(VIEWS):
            connection.execute(text(f"DROP VIEW IF EXISTS {name}"))
    logger.warning("Views de consumo removidas")
