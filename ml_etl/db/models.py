from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

JSONType = JSON().with_variant(JSONB, "postgresql")
SurrogateKey = BigInteger().with_variant(Integer, "sqlite")

EXTERNAL_TYPES = ("ITEM", "PRODUCT", "USER_PRODUCT")
SOURCES = ("HIGHLIGHTS_CATEGORY", "HIGHLIGHTS_PRODUCT", "TRENDS")
RUN_STATUSES = ("RUNNING", "SUCCESS", "FAILED")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class EtlRun(Base):
    __tablename__ = "etl_run"

    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="RUNNING", nullable=False)
    bucket_key: Mapped[str] = mapped_column(String(10), nullable=False)
    params: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    stats: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(f"status IN {RUN_STATUSES}", name="ck_etl_run_status"),
        Index("ix_etl_run_bucket", "bucket_key"),
        Index("ix_etl_run_started", "started_at"),
    )


class MarketRawResponse(Base):
    __tablename__ = "market_raw_response"

    id: Mapped[int] = mapped_column(SurrogateKey, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("etl_run.run_id", ondelete="CASCADE"), nullable=False)
    stage: Mapped[str] = mapped_column(String(20), nullable=False)
    source: Mapped[str | None] = mapped_column(String(30), nullable=True)
    endpoint: Mapped[str] = mapped_column(String(255), nullable=False)
    http_method: Mapped[str] = mapped_column(String(10), default="GET", nullable=False)
    request_params: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict | list | None] = mapped_column(JSONType, nullable=True)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    bucket_key: Mapped[str] = mapped_column(String(10), nullable=False)

    __table_args__ = (
        UniqueConstraint("run_id", "payload_hash", name="uq_raw_run_payload"),
        Index("ix_raw_run_stage", "run_id", "stage"),
        Index("ix_raw_bucket", "bucket_key"),
    )


class MarketCategory(Base, TimestampMixin):
    __tablename__ = "market_category"

    category_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    path_from_root: Mapped[list | None] = mapped_column(JSONType, nullable=True)
    total_items: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    permalink: Mapped[str | None] = mapped_column(Text, nullable=True)


class MarketEntity(Base, TimestampMixin):
    __tablename__ = "market_entity"

    id: Mapped[int] = mapped_column(SurrogateKey, primary_key=True, autoincrement=True)
    external_type: Mapped[str] = mapped_column(String(20), nullable=False)
    external_id: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    brand: Mapped[str | None] = mapped_column(String(120), nullable=True)
    price: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    available_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    condition: Mapped[str | None] = mapped_column(String(30), nullable=True)
    permalink: Mapped[str | None] = mapped_column(Text, nullable=True)
    attributes: Mapped[dict | list | None] = mapped_column(JSONType, nullable=True)
    enrich_status: Mapped[str] = mapped_column(String(20), default="OK", nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("external_type", "external_id", name="uq_entity_type_external_id"),
        CheckConstraint(f"external_type IN {EXTERNAL_TYPES}", name="ck_entity_external_type"),
        Index("ix_entity_category", "category_id"),
    )


class MarketSnapshot(Base, TimestampMixin):
    __tablename__ = "market_snapshot"

    id: Mapped[int] = mapped_column(SurrogateKey, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("etl_run.run_id", ondelete="SET NULL"), nullable=True)
    source: Mapped[str] = mapped_column(String(30), nullable=False)
    external_type: Mapped[str] = mapped_column(String(20), nullable=False)
    external_id: Mapped[str] = mapped_column(String(32), nullable=False)
    category_id: Mapped[str] = mapped_column(String(32), nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    rank_band: Mapped[str] = mapped_column(String(20), nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bucket_key: Mapped[str] = mapped_column(String(10), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "source", "external_type", "external_id", "category_id", "bucket_key", name="uq_snapshot_logical"
        ),
        CheckConstraint("rank BETWEEN 1 AND 20", name="ck_snapshot_rank_range"),
        CheckConstraint(f"external_type IN {EXTERNAL_TYPES}", name="ck_snapshot_external_type"),
        Index("ix_snapshot_bucket", "bucket_key"),
        Index("ix_snapshot_entity", "external_type", "external_id"),
    )


class MarketTrend(Base, TimestampMixin):
    __tablename__ = "market_trend"

    id: Mapped[int] = mapped_column(SurrogateKey, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("etl_run.run_id", ondelete="SET NULL"), nullable=True)
    category_id: Mapped[str] = mapped_column(String(32), nullable=False)
    keyword: Mapped[str] = mapped_column(String(255), nullable=False)
    canonical_keyword: Mapped[str | None] = mapped_column(String(255), nullable=True)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bucket_key: Mapped[str] = mapped_column(String(10), nullable=False)

    __table_args__ = (
        UniqueConstraint("category_id", "keyword", "bucket_key", name="uq_trend_logical"),
        Index("ix_trend_bucket", "bucket_key"),
        Index("ix_trend_canonical", "canonical_keyword"),
    )


class MarketScore(Base, TimestampMixin):
    __tablename__ = "market_score"

    id: Mapped[int] = mapped_column(SurrogateKey, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("etl_run.run_id", ondelete="SET NULL"), nullable=True)
    external_type: Mapped[str] = mapped_column(String(20), nullable=False)
    external_id: Mapped[str] = mapped_column(String(32), nullable=False)
    bucket_key: Mapped[str] = mapped_column(String(10), nullable=False)
    score: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)
    priority_label: Mapped[str] = mapped_column(String(20), nullable=False)
    confidence: Mapped[str] = mapped_column(String(10), nullable=False)
    components: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("external_type", "external_id", "bucket_key", name="uq_score_logical"),
        CheckConstraint("score >= 0 AND score <= 100", name="ck_score_range"),
        Index("ix_score_bucket", "bucket_key"),
    )


class MarketCuratedProduct(Base, TimestampMixin):
    __tablename__ = "market_curated_product"

    id: Mapped[int] = mapped_column(SurrogateKey, primary_key=True, autoincrement=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    external_type: Mapped[str] = mapped_column(String(20), nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    external_id: Mapped[str] = mapped_column(String(32), nullable=False)
    score: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)
    source: Mapped[str] = mapped_column(String(30), nullable=False)
    canonical_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    rank_band: Mapped[str] = mapped_column(String(20), nullable=False)
    rank_delta: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trajectory_label: Mapped[str | None] = mapped_column(String(30), nullable=True)
    category_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    brand: Mapped[str | None] = mapped_column(String(120), nullable=True)
    price: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    permalink: Mapped[str | None] = mapped_column(Text, nullable=True)
    priority_label: Mapped[str] = mapped_column(String(20), nullable=False)
    confidence: Mapped[str] = mapped_column(String(10), nullable=False)
    bucket_key: Mapped[str] = mapped_column(String(10), nullable=False)
    run_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("etl_run.run_id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("source", "external_type", "external_id", "bucket_key", name="uq_curated_logical"),
        CheckConstraint("rank BETWEEN 1 AND 20", name="ck_curated_rank_range"),
        CheckConstraint("score >= 0 AND score <= 100", name="ck_curated_score_range"),
        Index("ix_curated_bucket_score", "bucket_key", "score"),
        Index("ix_curated_category", "category_id"),
    )


class EtlRejection(Base):
    __tablename__ = "etl_rejection"

    id: Mapped[int] = mapped_column(SurrogateKey, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("etl_run.run_id", ondelete="CASCADE"), nullable=False)
    stage: Mapped[str] = mapped_column(String(20), nullable=False)
    layer: Mapped[str] = mapped_column(String(30), nullable=False)
    rule: Mapped[str] = mapped_column(String(120), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    external_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload: Mapped[dict | list | None] = mapped_column(JSONType, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_rejection_run_stage", "run_id", "stage"),
        Index("ix_rejection_severity", "severity"),
    )
