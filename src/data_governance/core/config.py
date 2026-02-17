"""Governance framework configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class ProfilerConfig(BaseModel):
    """Data profiler settings."""

    min_chunk_length: int = Field(default=20, description="Minimum acceptable chunk length (chars)")
    max_chunk_length: int = Field(
        default=5000, description="Maximum acceptable chunk length (chars)"
    )
    similarity_threshold: float = Field(
        default=0.95, description="Cosine similarity threshold for near-duplicate detection"
    )
    sample_size: int | None = Field(
        default=None, description="Sample size for profiling large collections (None = all)"
    )


class DedupConfig(BaseModel):
    """Deduplication settings."""

    hash_algorithm: str = Field(default="xxhash", description="Hash algorithm: xxhash | md5 | sha256")
    semantic_threshold: float = Field(
        default=0.95, description="Cosine similarity threshold for semantic dedup"
    )
    batch_size: int = Field(default=100, description="Batch size for processing")
    dry_run: bool = Field(default=True, description="If True, report but don't delete")


class FreshnessConfig(BaseModel):
    """Freshness tracking settings."""

    default_ttl_days: int = Field(default=30, description="Default TTL for short-term data (days)")
    long_term_review_days: int = Field(
        default=90, description="Review interval for long-term data (days)"
    )
    web_cache_ttl_days: int = Field(default=7, description="TTL for web cache data (days)")
    stale_threshold_days: int = Field(
        default=180, description="Days after which data is considered stale"
    )


class ValidationConfig(BaseModel):
    """Validation framework settings."""

    fail_on_error: bool = Field(default=False, description="Stop processing on first error")
    max_errors: int = Field(default=100, description="Max errors before aborting")
    custom_rules_path: str | None = Field(
        default=None, description="Path to custom validation rules YAML"
    )


class ReporterConfig(BaseModel):
    """Health reporter settings."""

    output_dir: str = Field(default="reports", description="Directory for report output")
    history_days: int = Field(default=30, description="Days of history to keep for trend analysis")
    alert_threshold: float = Field(
        default=0.6, description="Quality score below which alerts are triggered"
    )


class GovernanceConfig(BaseModel):
    """Root configuration for the data governance framework."""

    workspace_path: str = Field(
        default=".", description="Path to the workspace being governed"
    )
    governance_db_path: str = Field(
        default=".governance", description="Path to governance metadata store"
    )
    profiler: ProfilerConfig = Field(default_factory=ProfilerConfig)
    dedup: DedupConfig = Field(default_factory=DedupConfig)
    freshness: FreshnessConfig = Field(default_factory=FreshnessConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    reporter: ReporterConfig = Field(default_factory=ReporterConfig)

    @classmethod
    def from_file(cls, path: str | Path) -> GovernanceConfig:
        """Load config from a JSON file."""
        import json

        path = Path(path)
        if not path.exists():
            return cls()
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return cls(**data)

    def save(self, path: str | Path) -> None:
        """Save config to a JSON file."""
        import json

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.model_dump(), f, indent=2, ensure_ascii=False)

    def resolve_path(self, relative: str) -> Path:
        """Resolve a relative path against the workspace."""
        return Path(self.workspace_path).resolve() / relative

    def ensure_governance_db(self) -> Path:
        """Ensure governance metadata directory exists."""
        db_path = self.resolve_path(self.governance_db_path)
        db_path.mkdir(parents=True, exist_ok=True)
        return db_path
