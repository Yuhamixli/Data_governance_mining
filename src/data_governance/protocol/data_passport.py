"""Data Passport — a portable quality identity for every piece of data.

In the AI-first paradigm, data doesn't just exist — it carries an identity.
A Data Passport is a standardized quality certificate that travels with data
across systems, agents, and processing stages.

When Agent A passes knowledge to Agent B, the passport tells Agent B:
- Where this data came from (provenance)
- How trustworthy it is (quality scores)
- When it was last verified (freshness)
- What governance actions have been taken (audit trail)
- Whether it should be trusted for critical decisions (trust level)

This is the inter-agent data quality communication protocol.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class TrustLevel(str, Enum):
    """How much an agent should trust this data."""

    VERIFIED = "verified"       # Quality verified, freshness confirmed
    TRUSTED = "trusted"         # Quality acceptable, not recently verified
    UNVERIFIED = "unverified"   # No governance assessment yet
    SUSPECT = "suspect"         # Quality issues detected
    QUARANTINED = "quarantined" # Flagged for problems, do not use for decisions


class DataPassport(BaseModel):
    """Portable quality identity for a data asset.

    Every piece of data in the governance system gets a passport.
    The passport is the single source of truth about that data's quality,
    provenance, and trustworthiness.
    """

    # Identity
    passport_id: str = Field(description="Unique passport ID")
    asset_id: str = Field(description="ID of the data asset this passport belongs to")
    content_hash: str = Field(description="Hash of the content for integrity verification")

    # Provenance
    source_type: str = Field(description="Origin type: file, web, chat, memory, api")
    source_path: str = Field(default="", description="Source location")
    source_url: str | None = Field(default=None, description="Source URL if from web")
    created_at: datetime = Field(default_factory=datetime.now)
    ingested_at: datetime | None = Field(default=None)

    # Quality
    quality_score: float = Field(ge=0.0, le=1.0, default=0.0)
    freshness_score: float = Field(ge=0.0, le=1.0, default=0.0)
    trust_level: TrustLevel = Field(default=TrustLevel.UNVERIFIED)

    # Quality dimensions
    completeness: float = Field(ge=0.0, le=1.0, default=0.0)
    validity: float = Field(ge=0.0, le=1.0, default=0.0)
    uniqueness: float = Field(ge=0.0, le=1.0, default=0.0)

    # Governance audit trail
    governance_actions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="History of governance actions taken on this data",
    )
    last_assessed: datetime | None = Field(default=None)
    assessment_count: int = Field(default=0)

    # Agent communication metadata
    tags: list[str] = Field(default_factory=list)
    notes: str = Field(default="")

    def record_action(self, action: str, agent_id: str = "governance_agent", details: dict | None = None) -> None:
        """Record a governance action in the audit trail."""
        self.governance_actions.append({
            "action": action,
            "agent_id": agent_id,
            "timestamp": datetime.now().isoformat(),
            "details": details or {},
        })

    def assess(
        self,
        quality_score: float,
        freshness_score: float,
        completeness: float = 0.0,
        validity: float = 0.0,
        uniqueness: float = 0.0,
    ) -> None:
        """Update quality assessment."""
        self.quality_score = quality_score
        self.freshness_score = freshness_score
        self.completeness = completeness
        self.validity = validity
        self.uniqueness = uniqueness
        self.last_assessed = datetime.now()
        self.assessment_count += 1

        # Auto-determine trust level
        if quality_score >= 0.8 and freshness_score >= 0.5:
            self.trust_level = TrustLevel.VERIFIED
        elif quality_score >= 0.6:
            self.trust_level = TrustLevel.TRUSTED
        elif quality_score >= 0.3:
            self.trust_level = TrustLevel.SUSPECT
        else:
            self.trust_level = TrustLevel.QUARANTINED

    def to_metadata(self) -> dict[str, Any]:
        """Export as flat metadata dict (for embedding in ChromaDB metadata)."""
        return {
            "passport_id": self.passport_id,
            "quality_score": self.quality_score,
            "freshness_score": self.freshness_score,
            "trust_level": self.trust_level.value,
            "source_type": self.source_type,
            "last_assessed": self.last_assessed.isoformat() if self.last_assessed else None,
            "assessment_count": self.assessment_count,
        }

    def to_agent_context(self) -> str:
        """Format for inclusion in an agent's context/prompt.

        This tells the consuming agent exactly how to weight this data.
        """
        return (
            f"[Data: {self.asset_id} | "
            f"Trust: {self.trust_level.value} | "
            f"Quality: {self.quality_score:.0%} | "
            f"Fresh: {self.freshness_score:.0%} | "
            f"Source: {self.source_type}]"
        )


class PassportRegistry:
    """Registry for managing Data Passports.

    Provides CRUD operations and bulk assessment capabilities.
    Persists passport data for cross-session continuity.
    """

    def __init__(self, persist_path: str | Path | None = None):
        self.passports: dict[str, DataPassport] = {}
        self.persist_path = Path(persist_path) if persist_path else None
        if self.persist_path and self.persist_path.exists():
            self._load()

    def create_passport(
        self,
        asset_id: str,
        content: str,
        source_type: str,
        source_path: str = "",
        **kwargs: Any,
    ) -> DataPassport:
        """Create a new passport for a data asset."""
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]
        passport_id = f"pp_{content_hash}"

        passport = DataPassport(
            passport_id=passport_id,
            asset_id=asset_id,
            content_hash=content_hash,
            source_type=source_type,
            source_path=source_path,
            **kwargs,
        )

        self.passports[asset_id] = passport
        self._save()
        return passport

    def get_passport(self, asset_id: str) -> DataPassport | None:
        return self.passports.get(asset_id)

    def assess_all(
        self,
        assessments: dict[str, dict[str, float]],
    ) -> int:
        """Bulk quality assessment.

        Args:
            assessments: Dict of asset_id -> {quality_score, freshness_score, ...}
        """
        updated = 0
        for asset_id, scores in assessments.items():
            passport = self.passports.get(asset_id)
            if passport:
                passport.assess(**scores)
                updated += 1
        self._save()
        return updated

    def get_trusted(self, min_trust: TrustLevel = TrustLevel.TRUSTED) -> list[DataPassport]:
        """Get all passports at or above a trust level."""
        trust_order = {
            TrustLevel.QUARANTINED: 0,
            TrustLevel.SUSPECT: 1,
            TrustLevel.UNVERIFIED: 2,
            TrustLevel.TRUSTED: 3,
            TrustLevel.VERIFIED: 4,
        }
        min_level = trust_order.get(min_trust, 0)
        return [
            p for p in self.passports.values()
            if trust_order.get(p.trust_level, 0) >= min_level
        ]

    def get_quarantined(self) -> list[DataPassport]:
        """Get all quarantined data passports."""
        return [p for p in self.passports.values() if p.trust_level == TrustLevel.QUARANTINED]

    def stats(self) -> dict[str, Any]:
        """Get registry statistics for agent consumption."""
        total = len(self.passports)
        if total == 0:
            return {"total": 0}

        trust_dist = {}
        for p in self.passports.values():
            trust_dist[p.trust_level.value] = trust_dist.get(p.trust_level.value, 0) + 1

        scores = [p.quality_score for p in self.passports.values()]
        return {
            "total": total,
            "avg_quality": sum(scores) / total,
            "trust_distribution": trust_dist,
            "quarantined_count": trust_dist.get("quarantined", 0),
            "verified_count": trust_dist.get("verified", 0),
            "assessed_count": sum(1 for p in self.passports.values() if p.last_assessed),
        }

    def _save(self) -> None:
        if not self.persist_path:
            return
        self.persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            asset_id: passport.model_dump(mode="json")
            for asset_id, passport in self.passports.items()
        }
        with open(self.persist_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    def _load(self) -> None:
        try:
            data = json.loads(self.persist_path.read_text(encoding="utf-8"))
            self.passports = {}
            for asset_id, pdata in data.items():
                self.passports[asset_id] = DataPassport(**pdata)
        except (json.JSONDecodeError, KeyError, TypeError):
            self.passports = {}
