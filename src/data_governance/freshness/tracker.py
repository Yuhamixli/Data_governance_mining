"""Freshness tracking for data assets — detects stale, expired, and outdated data."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.freshness.policies import ExpirationAction, FreshnessPolicy


@dataclass
class FreshnessRecord:
    """Tracks freshness state for a single data asset."""

    asset_id: str
    asset_name: str
    source_path: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_verified: datetime | None = None
    ttl_days: int | None = None
    is_stale: bool = False
    is_expired: bool = False
    freshness_score: float = 1.0
    category: str = "unknown"

    @property
    def age_days(self) -> float | None:
        if self.updated_at:
            return (datetime.now() - self.updated_at).total_seconds() / 86400
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "asset_name": self.asset_name,
            "source_path": self.source_path,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "last_verified": self.last_verified.isoformat() if self.last_verified else None,
            "ttl_days": self.ttl_days,
            "is_stale": self.is_stale,
            "is_expired": self.is_expired,
            "freshness_score": self.freshness_score,
            "category": self.category,
        }


@dataclass
class FreshnessReport:
    """Aggregate freshness report."""

    records: list[FreshnessRecord] = field(default_factory=list)
    scan_time: datetime = field(default_factory=datetime.now)

    @property
    def total(self) -> int:
        return len(self.records)

    @property
    def stale_count(self) -> int:
        return sum(1 for r in self.records if r.is_stale)

    @property
    def expired_count(self) -> int:
        return sum(1 for r in self.records if r.is_expired)

    @property
    def avg_freshness(self) -> float:
        if not self.records:
            return 0.0
        return sum(r.freshness_score for r in self.records) / len(self.records)

    def summary(self) -> str:
        lines = [
            f"Freshness Report ({self.scan_time.strftime('%Y-%m-%d %H:%M')})",
            f"Total assets: {self.total}",
            f"Average freshness: {self.avg_freshness:.1%}",
            f"Stale: {self.stale_count} | Expired: {self.expired_count}",
        ]
        if self.stale_count > 0:
            lines.append("\nStale assets:")
            for r in self.records:
                if r.is_stale:
                    age = f"{r.age_days:.0f}d" if r.age_days else "?"
                    lines.append(f"  - {r.asset_name} (age: {age}, score: {r.freshness_score:.1%})")
        if self.expired_count > 0:
            lines.append("\nExpired assets:")
            for r in self.records:
                if r.is_expired:
                    age = f"{r.age_days:.0f}d" if r.age_days else "?"
                    lines.append(f"  - {r.asset_name} (age: {age})")
        return "\n".join(lines)


class FreshnessTracker:
    """Track and manage data freshness across the knowledge base.

    Scans files, vector store metadata, and chat histories to determine
    which data is fresh, stale, or expired, and recommends actions.
    """

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()
        self.policies: list[FreshnessPolicy] = self._default_policies()
        self._state_file: Path | None = None

    def _default_policies(self) -> list[FreshnessPolicy]:
        """Create default freshness policies based on config."""
        return [
            FreshnessPolicy(
                name="short_term_knowledge",
                category="short_term",
                ttl_days=self.config.freshness.default_ttl_days,
                stale_days=self.config.freshness.default_ttl_days // 2,
                action=ExpirationAction.ARCHIVE,
            ),
            FreshnessPolicy(
                name="web_cache",
                category="web_cache",
                ttl_days=self.config.freshness.web_cache_ttl_days,
                stale_days=self.config.freshness.web_cache_ttl_days // 2,
                action=ExpirationAction.DELETE,
            ),
            FreshnessPolicy(
                name="long_term_knowledge",
                category="long_term",
                ttl_days=None,  # No expiration
                stale_days=self.config.freshness.long_term_review_days,
                action=ExpirationAction.REVIEW,
            ),
            FreshnessPolicy(
                name="chat_history",
                category="chat_history",
                ttl_days=self.config.freshness.stale_threshold_days,
                stale_days=90,
                action=ExpirationAction.ARCHIVE,
            ),
            FreshnessPolicy(
                name="daily_memory",
                category="memory",
                ttl_days=None,
                stale_days=self.config.freshness.stale_threshold_days,
                action=ExpirationAction.REVIEW,
            ),
        ]

    def scan_knowledge_dir(self, knowledge_dir: str | Path) -> FreshnessReport:
        """Scan knowledge directory for freshness.

        Args:
            knowledge_dir: Path to the knowledge directory.
        """
        knowledge_dir = Path(knowledge_dir)
        records: list[FreshnessRecord] = []
        now = datetime.now()

        for path in sorted(knowledge_dir.rglob("*")):
            if not path.is_file():
                continue

            # Determine category from path structure
            rel = path.relative_to(knowledge_dir)
            parts = rel.parts
            if len(parts) > 0 and parts[0] in ("短期", "short_term"):
                if "_cache_web" in str(rel):
                    category = "web_cache"
                else:
                    category = "short_term"
            elif len(parts) > 0 and parts[0] in ("长期", "long_term"):
                category = "long_term"
            else:
                category = "unknown"

            try:
                stat = path.stat()
                updated_at = datetime.fromtimestamp(stat.st_mtime)
                created_at = datetime.fromtimestamp(stat.st_ctime)
            except OSError:
                updated_at = None
                created_at = None

            policy = self._get_policy(category)
            record = FreshnessRecord(
                asset_id=f"file:{rel}",
                asset_name=path.name,
                source_path=str(path),
                created_at=created_at,
                updated_at=updated_at,
                last_verified=now,
                ttl_days=policy.ttl_days if policy else None,
                category=category,
            )

            if updated_at and policy:
                age_days = (now - updated_at).total_seconds() / 86400
                record.is_stale = policy.stale_days is not None and age_days > policy.stale_days
                record.is_expired = policy.ttl_days is not None and age_days > policy.ttl_days
                if policy.ttl_days:
                    record.freshness_score = max(0.0, 1.0 - age_days / policy.ttl_days)
                elif policy.stale_days:
                    record.freshness_score = max(0.0, 1.0 - age_days / (policy.stale_days * 2))
                else:
                    record.freshness_score = 1.0

            records.append(record)

        return FreshnessReport(records=records, scan_time=now)

    def scan_memory_dir(self, memory_dir: str | Path) -> FreshnessReport:
        """Scan memory directory for freshness."""
        memory_dir = Path(memory_dir)
        records: list[FreshnessRecord] = []
        now = datetime.now()
        policy = self._get_policy("memory")

        for path in sorted(memory_dir.rglob("*.md")):
            if not path.is_file():
                continue

            try:
                stat = path.stat()
                updated_at = datetime.fromtimestamp(stat.st_mtime)
            except OSError:
                updated_at = None

            record = FreshnessRecord(
                asset_id=f"memory:{path.name}",
                asset_name=path.name,
                source_path=str(path),
                updated_at=updated_at,
                last_verified=now,
                category="memory",
            )

            if updated_at and policy:
                age_days = (now - updated_at).total_seconds() / 86400
                record.is_stale = policy.stale_days is not None and age_days > policy.stale_days
                if policy.stale_days:
                    record.freshness_score = max(0.0, 1.0 - age_days / (policy.stale_days * 2))

            records.append(record)

        return FreshnessReport(records=records, scan_time=now)

    def get_expired_assets(self, report: FreshnessReport) -> list[dict[str, Any]]:
        """Get list of expired assets with recommended actions."""
        expired = []
        for r in report.records:
            if r.is_expired:
                policy = self._get_policy(r.category)
                expired.append({
                    "asset_id": r.asset_id,
                    "name": r.asset_name,
                    "path": r.source_path,
                    "age_days": r.age_days,
                    "action": policy.action.value if policy else "review",
                })
        return expired

    def get_stale_assets(self, report: FreshnessReport) -> list[dict[str, Any]]:
        """Get list of stale (but not expired) assets."""
        stale = []
        for r in report.records:
            if r.is_stale and not r.is_expired:
                stale.append({
                    "asset_id": r.asset_id,
                    "name": r.asset_name,
                    "path": r.source_path,
                    "age_days": r.age_days,
                    "freshness_score": r.freshness_score,
                })
        return stale

    def save_state(self, output_path: str | Path) -> None:
        """Persist freshness state for trend analysis."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "scan_time": datetime.now().isoformat(),
            "records": [],
        }

        # Load existing
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(existing, list):
                    state["records"] = existing
                elif "records" in existing:
                    state["records"] = existing["records"]
            except (json.JSONDecodeError, KeyError):
                pass

        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False, default=str)

    def _get_policy(self, category: str) -> FreshnessPolicy | None:
        """Find the matching policy for a category."""
        for p in self.policies:
            if p.category == category:
                return p
        return None
