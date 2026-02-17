"""Health reporting — aggregates all governance signals into a unified health dashboard."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.core.models import (
    DataAssetType,
    QualityDimension,
    QualityReport,
    QualityScore,
    ValidationResult,
    ValidationSeverity,
)
from data_governance.dedup.hash_dedup import DedupResult
from data_governance.freshness.tracker import FreshnessReport


class HealthReporter:
    """Aggregates all governance signals into a unified health score.

    Combines profiling, validation, deduplication, and freshness data
    to produce an overall knowledge base health assessment.
    """

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()

    def compute_health(
        self,
        quality_report: QualityReport | None = None,
        validation_results: list[ValidationResult] | None = None,
        dedup_result: DedupResult | None = None,
        freshness_report: FreshnessReport | None = None,
    ) -> HealthScore:
        """Compute overall health score from governance signals.

        Each signal contributes a weighted score to the overall health.
        """
        components: list[HealthComponent] = []

        # Quality score
        if quality_report:
            components.append(
                HealthComponent(
                    name="Data Quality",
                    score=quality_report.overall_score,
                    weight=0.35,
                    details=f"{quality_report.total_items} items, {quality_report.issues_found} issues",
                    recommendations=quality_report.recommendations,
                )
            )

        # Validation score
        if validation_results:
            total = len(validation_results)
            passed = sum(1 for r in validation_results if r.passed)
            errors = sum(
                1
                for r in validation_results
                if not r.passed and r.severity == ValidationSeverity.ERROR
            )
            val_score = passed / total if total > 0 else 1.0
            components.append(
                HealthComponent(
                    name="Validation",
                    score=val_score,
                    weight=0.25,
                    details=f"{passed}/{total} passed, {errors} errors",
                )
            )

        # Uniqueness score
        if dedup_result:
            dup_ratio = dedup_result.duplicate_ratio
            components.append(
                HealthComponent(
                    name="Uniqueness",
                    score=1.0 - dup_ratio,
                    weight=0.20,
                    details=f"{dedup_result.duplicate_count} duplicates out of {dedup_result.total_items}",
                )
            )

        # Freshness score
        if freshness_report:
            components.append(
                HealthComponent(
                    name="Freshness",
                    score=freshness_report.avg_freshness,
                    weight=0.20,
                    details=f"{freshness_report.stale_count} stale, {freshness_report.expired_count} expired",
                )
            )

        if not components:
            return HealthScore(
                overall=0.0,
                components=[],
                generated_at=datetime.now(),
                summary="No governance data available",
            )

        # Normalize weights
        total_weight = sum(c.weight for c in components)
        overall = sum(c.score * c.weight for c in components) / total_weight

        # Determine status
        if overall >= 0.8:
            status = "healthy"
        elif overall >= 0.6:
            status = "warning"
        else:
            status = "critical"

        # Collect all recommendations
        all_recs = []
        for c in components:
            all_recs.extend(c.recommendations)

        return HealthScore(
            overall=overall,
            status=status,
            components=components,
            generated_at=datetime.now(),
            recommendations=all_recs,
            summary=self._generate_summary(overall, status, components),
        )

    def generate_report(
        self,
        health: HealthScore,
        output_path: str | Path | None = None,
    ) -> str:
        """Generate a detailed health report.

        Args:
            health: Computed health score.
            output_path: Optional path to save the report.
        """
        report = health.to_markdown()

        if output_path:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(report, encoding="utf-8")

        return report

    def save_history(self, health: HealthScore, history_path: str | Path) -> None:
        """Append health score to history for trend analysis."""
        path = Path(history_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        entry = {
            "timestamp": health.generated_at.isoformat(),
            "overall": health.overall,
            "status": health.status,
            "components": {c.name: c.score for c in health.components},
        }

        # Append as JSONL
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def load_history(self, history_path: str | Path) -> list[dict[str, Any]]:
        """Load health score history."""
        path = Path(history_path)
        if not path.exists():
            return []

        entries = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return entries

    def _generate_summary(
        self, overall: float, status: str, components: list[HealthComponent]
    ) -> str:
        """Generate a concise text summary."""
        status_emoji = {"healthy": "OK", "warning": "WARN", "critical": "CRIT"}
        lines = [
            f"Knowledge Base Health: {overall:.1%} [{status_emoji.get(status, status)}]",
            "",
        ]
        for c in sorted(components, key=lambda x: x.score):
            bar = "█" * int(c.score * 10) + "░" * (10 - int(c.score * 10))
            lines.append(f"  {c.name:15s} {bar} {c.score:.1%} — {c.details}")
        return "\n".join(lines)


class HealthComponent:
    """A single component of the health score."""

    def __init__(
        self,
        name: str,
        score: float,
        weight: float = 1.0,
        details: str = "",
        recommendations: list[str] | None = None,
    ):
        self.name = name
        self.score = max(0.0, min(1.0, score))
        self.weight = weight
        self.details = details
        self.recommendations = recommendations or []


class HealthScore:
    """Overall health score with component breakdown."""

    def __init__(
        self,
        overall: float,
        components: list[HealthComponent],
        generated_at: datetime | None = None,
        status: str = "unknown",
        summary: str = "",
        recommendations: list[str] | None = None,
    ):
        self.overall = max(0.0, min(1.0, overall))
        self.components = components
        self.generated_at = generated_at or datetime.now()
        self.status = status
        self.summary = summary
        self.recommendations = recommendations or []

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall": self.overall,
            "status": self.status,
            "generated_at": self.generated_at.isoformat(),
            "components": [
                {"name": c.name, "score": c.score, "weight": c.weight, "details": c.details}
                for c in self.components
            ],
            "recommendations": self.recommendations,
        }

    def to_markdown(self) -> str:
        """Generate a markdown report."""
        lines = [
            "# Knowledge Base Health Report",
            "",
            f"**Generated:** {self.generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Overall Health:** {self.overall:.1%} ({self.status.upper()})",
            "",
            "## Component Scores",
            "",
            "| Component | Score | Weight | Details |",
            "|-----------|-------|--------|---------|",
        ]
        for c in self.components:
            lines.append(f"| {c.name} | {c.score:.1%} | {c.weight:.0%} | {c.details} |")

        if self.recommendations:
            lines.extend(["", "## Recommendations", ""])
            for i, rec in enumerate(self.recommendations, 1):
                lines.append(f"{i}. {rec}")

        lines.append("")
        return "\n".join(lines)
