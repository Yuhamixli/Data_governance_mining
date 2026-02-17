"""Freshness policies — define TTL, staleness thresholds, and actions."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class ExpirationAction(str, Enum):
    """What to do when data expires."""

    DELETE = "delete"
    ARCHIVE = "archive"
    REVIEW = "review"
    NOTIFY = "notify"


class FreshnessPolicy(BaseModel):
    """Defines freshness rules for a category of data assets.

    Attributes:
        name: Policy name.
        category: Data category this policy applies to.
        ttl_days: Time-to-live in days (None = no expiration).
        stale_days: Days after which data is considered stale.
        action: What to do when data expires.
    """

    name: str = Field(description="Policy name")
    category: str = Field(description="Data category (short_term, long_term, web_cache, etc.)")
    ttl_days: int | None = Field(default=None, description="TTL in days (None = no expiration)")
    stale_days: int | None = Field(default=None, description="Stale threshold in days")
    action: ExpirationAction = Field(
        default=ExpirationAction.REVIEW, description="Action on expiration"
    )
    enabled: bool = Field(default=True)

    def is_stale(self, age_days: float) -> bool:
        """Check if data of this category's age is stale."""
        if self.stale_days is None:
            return False
        return age_days > self.stale_days

    def is_expired(self, age_days: float) -> bool:
        """Check if data of this category's age is expired."""
        if self.ttl_days is None:
            return False
        return age_days > self.ttl_days
