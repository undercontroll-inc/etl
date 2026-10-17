from __future__ import annotations

from sqlalchemy import Connection, Engine, text

from ml_etl.domain.buckets import RISING_TRAJECTORIES
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

RISING_SQL = "(" + ", ".join(f"'{label}'" for label in RISING_TRAJECTORIES) + ")"
REFERENCE_RUN_SQL = "(SELECT run_id FROM vw_market_published_snapshot)"

VW_PUBLISHED_SNAPSHOT = """
CREATE VIEW vw_market_published_snapshot AS
SELECT
    snapshot_id,
    run_id,
    bucket_key,
    status,
    quality_report,
    published_at
FROM market_published_snapshot
ORDER BY published_at DESC, snapshot_id DESC
LIMIT 1
"""

VW_PRODUCT_CURRENT = f"""
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
    c.available_offer_count,
    c.observed_offer_count,
    c.valid_price_count,
    c.sample_complete,
    c.observed_seller_count,
    c.relevance,
    c.domain_reason,
    c.permalink,
    c.collected_at
FROM market_curated_product c
WHERE c.run_id = {REFERENCE_RUN_SQL}
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
    available_offer_count,
    observed_offer_count,
    valid_price_count,
    sample_complete,
    observed_seller_count,
    capacity_value,
    capacity_unit,
    power_watts,
    voltage,
    permalink
FROM vw_market_product_current
WHERE trajectory_label IN {RISING_SQL}
   OR priority_label = 'PRIORIDADE_ALTA'
"""

VW_CATEGORY_SUMMARY = f"""
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
    ROUND(CAST(AVG(valid_price_count) AS NUMERIC), 2) AS avg_valid_price_count,
    ROUND(CAST(AVG(price_delta_pct) AS NUMERIC), 2) AS avg_price_delta_pct,
    ROUND(CAST(AVG(discount_pct) AS NUMERIC), 2) AS avg_discount_pct,
    SUM(CASE WHEN trajectory_label IN {RISING_SQL} THEN 1 ELSE 0 END) AS rising_count,
    SUM(CASE WHEN trajectory_label = 'QUEDA' THEN 1 ELSE 0 END) AS falling_count,
    SUM(CASE WHEN confidence = 'HIGH' THEN 1 ELSE 0 END) AS high_confidence_count
FROM market_curated_product
WHERE domain_id IS NOT NULL
  AND run_id = {REFERENCE_RUN_SQL}
GROUP BY domain_id, bucket_key
"""

VW_BRAND_SUMMARY = f"""
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
    SUM(CASE WHEN trajectory_label IN {RISING_SQL} THEN 1 ELSE 0 END) AS rising_count
FROM market_curated_product
WHERE brand_slug IS NOT NULL
  AND run_id = {REFERENCE_RUN_SQL}
GROUP BY brand_slug, bucket_key
"""

VW_PRICE_MOVEMENT = """
CREATE VIEW vw_market_price_movement AS
WITH domain_bucket AS (
    SELECT
        p.domain_id AS domain_id,
        p.bucket_key AS bucket_key,
        COUNT(*) AS product_count,
        SUM(p.valid_price_count) AS valid_price_count,
        SUM(p.observed_seller_count) AS observed_seller_count,
        AVG(p.price_median) AS avg_price_median
    FROM market_price_observation p
    WHERE p.domain_id IS NOT NULL
      AND p.price_median IS NOT NULL
      AND p.run_id = (
          SELECT s.run_id
          FROM market_published_snapshot s
          JOIN market_price_observation x ON x.run_id = s.run_id
          WHERE x.bucket_key = p.bucket_key
          ORDER BY s.published_at DESC, s.snapshot_id DESC
          LIMIT 1
      )
    GROUP BY p.domain_id, p.bucket_key
),
with_previous AS (
    SELECT
        domain_id,
        bucket_key,
        product_count,
        valid_price_count,
        observed_seller_count,
        avg_price_median,
        LAG(avg_price_median) OVER (PARTITION BY domain_id ORDER BY bucket_key) AS previous_price_median,
        LAG(bucket_key) OVER (PARTITION BY domain_id ORDER BY bucket_key) AS previous_bucket_key
    FROM domain_bucket
)
SELECT
    domain_id,
    bucket_key,
    previous_bucket_key,
    product_count,
    valid_price_count,
    observed_seller_count,
    ROUND(CAST(avg_price_median AS NUMERIC), 2) AS avg_price_median,
    ROUND(CAST(previous_price_median AS NUMERIC), 2) AS previous_price_median,
    CASE
        WHEN previous_price_median IS NULL OR previous_price_median <= 0 THEN NULL
        ELSE ROUND(CAST((avg_price_median - previous_price_median) / previous_price_median * 100 AS NUMERIC), 2)
    END AS price_delta_pct
FROM with_previous
"""

VIEWS: tuple[tuple[str, str], ...] = (
    ("vw_market_published_snapshot", VW_PUBLISHED_SNAPSHOT),
    ("vw_market_product_current", VW_PRODUCT_CURRENT),
    ("vw_market_rising_products", VW_RISING_PRODUCTS),
    ("vw_market_category_summary", VW_CATEGORY_SUMMARY),
    ("vw_market_brand_summary", VW_BRAND_SUMMARY),
    ("vw_market_price_movement", VW_PRICE_MOVEMENT),
)


def drop_views_on(connection: Connection) -> None:
    for name, _ in reversed(VIEWS):
        connection.execute(text(f"DROP VIEW IF EXISTS {name}"))


def create_views(engine: Engine) -> list[str]:
    created: list[str] = []
    with engine.begin() as connection:
        drop_views_on(connection)
        for name, statement in VIEWS:
            connection.execute(text(statement))
            created.append(name)
    logger.info("Views de consumo criadas: %s", ", ".join(created))
    return created


def drop_views(engine: Engine) -> None:
    with engine.begin() as connection:
        drop_views_on(connection)
    logger.warning("Views de consumo removidas")
