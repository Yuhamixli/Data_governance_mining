"""Alert management — generates and tracks governance alerts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    """A governance alert."""

    id: str
    level: AlertLevel
    title: str
    message: str
    source: str = ""
    asset_id: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    acknowledged: bool = False
    resolved: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "level": self.level.value,
            "title": self.title,
            "message": self.message,
            "source": self.source,
            "asset_id": self.asset_id,
            "created_at": self.created_at.isoformat(),
            "acknowledged": self.acknowledged,
            "resolved": self.resolved,
        }


class AlertManager:
    """Manages governance alerts.

    Generates alerts based on health scores, validation results,
    and freshness checks. Persists alert history for tracking.
    """

    def __init__(self, persist_path: str | Path | None = None):
        self.alerts: list[Alert] = []
        self.persist_path = Path(persist_path) if persist_path else None
        if self.persist_path and self.persist_path.exists():
            self._load()

    def check_health_alerts(
        self,
        overall_score: float,
        threshold: float = 0.6,
        component_scores: dict[str, float] | None = None,
    ) -> list[Alert]:
        """Generate alerts based on health scores.

        Args:
            overall_score: Overall health score (0-1).
            threshold: Score below which to alert.
            component_scores: Individual component scores.
        """
        new_alerts: list[Alert] = []

        if overall_score < threshold:
            level = AlertLevel.CRITICAL if overall_score < 0.4 else AlertLevel.WARNING
            alert = Alert(
                id=f"health_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                level=level,
                title="Knowledge Base Health Below Threshold",
                message=(
                    f"Overall health score is {overall_score:.1%}, "
                    f"below the threshold of {threshold:.1%}"
                ),
                source="health_reporter",
            )
            new_alerts.append(alert)

        if component_scores:
            for name, score in component_scores.items():
                if score < 0.5:
                    alert = Alert(
                        id=f"component_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        level=AlertLevel.WARNING,
                        title=f"Low {name} Score",
                        message=f"{name} score is {score:.1%} — needs attention",
                        source="health_reporter",
                    )
                    new_alerts.append(alert)

        self.alerts.extend(new_alerts)
        self._save()
        return new_alerts

    def check_freshness_alerts(
        self,
        stale_count: int,
        expired_count: int,
        total_count: int,
    ) -> list[Alert]:
        """Generate alerts for freshness issues."""
        new_alerts: list[Alert] = []

        if expired_count > 0:
            alert = Alert(
                id=f"freshness_expired_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                level=AlertLevel.WARNING,
                title="Expired Data Detected",
                message=f"{expired_count} out of {total_count} assets have expired",
                source="freshness_tracker",
            )
            new_alerts.append(alert)

        if stale_count > total_count * 0.3:
            alert = Alert(
                id=f"freshness_stale_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                level=AlertLevel.WARNING,
                title="High Stale Data Ratio",
                message=f"{stale_count} out of {total_count} assets are stale ({stale_count/total_count:.0%})",
                source="freshness_tracker",
            )
            new_alerts.append(alert)

        self.alerts.extend(new_alerts)
        self._save()
        return new_alerts

    def check_dedup_alerts(
        self, duplicate_count: int, total_count: int
    ) -> list[Alert]:
        """Generate alerts for deduplication issues."""
        new_alerts: list[Alert] = []

        if total_count > 0 and duplicate_count / total_count > 0.1:
            alert = Alert(
                id=f"dedup_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                level=AlertLevel.WARNING,
                title="High Duplicate Ratio",
                message=(
                    f"{duplicate_count} duplicates out of {total_count} items "
                    f"({duplicate_count/total_count:.0%})"
                ),
                source="dedup_engine",
            )
            new_alerts.append(alert)

        self.alerts.extend(new_alerts)
        self._save()
        return new_alerts

    def get_active_alerts(self) -> list[Alert]:
        """Get all unresolved alerts."""
        return [a for a in self.alerts if not a.resolved]

    def acknowledge(self, alert_id: str) -> bool:
        """Mark an alert as acknowledged."""
        for a in self.alerts:
            if a.id == alert_id:
                a.acknowledged = True
                self._save()
                return True
        return False

    def resolve(self, alert_id: str) -> bool:
        """Mark an alert as resolved."""
        for a in self.alerts:
            if a.id == alert_id:
                a.resolved = True
                self._save()
                return True
        return False

    def summary(self) -> str:
        """Generate alert summary."""
        active = self.get_active_alerts()
        critical = sum(1 for a in active if a.level == AlertLevel.CRITICAL)
        warnings = sum(1 for a in active if a.level == AlertLevel.WARNING)
        infos = sum(1 for a in active if a.level == AlertLevel.INFO)

        lines = [
            f"Active Alerts: {len(active)} (Critical: {critical}, Warning: {warnings}, Info: {infos})",
        ]
        for a in active:
            marker = {"critical": "[!]", "warning": "[*]", "info": "[i]"}
            lines.append(f"  {marker.get(a.level.value, '[-]')} {a.title}: {a.message}")

        return "\n".join(lines)

    def _save(self) -> None:
        if not self.persist_path:
            return
        self.persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = [a.to_dict() for a in self.alerts]
        with open(self.persist_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _load(self) -> None:
        try:
            data = json.loads(self.persist_path.read_text(encoding="utf-8"))
            self.alerts = []
            for d in data:
                d["level"] = AlertLevel(d["level"])
                d["created_at"] = datetime.fromisoformat(d["created_at"])
                self.alerts.append(Alert(**d))
        except (json.JSONDecodeError, KeyError, TypeError):
            self.alerts = []
