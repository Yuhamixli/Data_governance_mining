"""Decision models for autonomous governance agent.

Unlike traditional governance that produces reports for humans,
these models represent machine-actionable decisions that an AI agent
can reason about, execute, and learn from.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DecisionType(str, Enum):
    """Types of governance decisions an agent can make."""

    # Immediate auto-remediation actions
    DELETE_DUPLICATES = "delete_duplicates"
    DELETE_EMPTY_CHUNKS = "delete_empty_chunks"
    DELETE_EXPIRED = "delete_expired"
    ARCHIVE_STALE = "archive_stale"
    RE_INGEST = "re_ingest"
    QUARANTINE = "quarantine"

    # Quality enhancement actions
    ENRICH_METADATA = "enrich_metadata"
    NORMALIZE_PATHS = "normalize_paths"
    RE_CHUNK = "re_chunk"
    UPDATE_EMBEDDINGS = "update_embeddings"

    # Monitoring actions
    ESCALATE = "escalate"
    SCHEDULE_REVIEW = "schedule_review"
    ADJUST_POLICY = "adjust_policy"

    # No action needed
    NO_ACTION = "no_action"


class Severity(str, Enum):
    """How urgently this decision needs to be acted on."""

    CRITICAL = "critical"  # Act immediately, data integrity at risk
    HIGH = "high"          # Act soon, quality degrading
    MEDIUM = "medium"      # Act when convenient
    LOW = "low"            # Informational, may self-resolve


class Decision(BaseModel):
    """A single governance decision made by the agent.

    This is not a report for a human — it's a structured action
    that another AI agent can parse, reason about, and execute.
    """

    id: str = Field(description="Unique decision ID")
    decision_type: DecisionType
    severity: Severity = Severity.MEDIUM
    confidence: float = Field(ge=0.0, le=1.0, description="Agent's confidence in this decision")

    # What triggered this decision
    trigger: str = Field(description="What signal triggered this decision")
    evidence: dict[str, Any] = Field(default_factory=dict, description="Supporting data")

    # What to do
    target_ids: list[str] = Field(default_factory=list, description="IDs of affected data assets")
    action_params: dict[str, Any] = Field(default_factory=dict, description="Parameters for action")

    # Reasoning
    reasoning: str = Field(default="", description="Why this decision was made")
    expected_impact: str = Field(default="", description="Expected outcome after action")

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    executed: bool = False
    execution_result: dict[str, Any] | None = None

    def to_agent_message(self) -> str:
        """Format as a concise message another agent can understand."""
        return (
            f"[{self.severity.value.upper()}] {self.decision_type.value}: "
            f"{self.reasoning} "
            f"(confidence: {self.confidence:.0%}, targets: {len(self.target_ids)})"
        )


class ActionPlan(BaseModel):
    """An ordered plan of governance actions the agent intends to take.

    The plan is structured so that:
    1. Another agent can review it before execution
    2. Actions can be executed in sequence with dependency tracking
    3. Results feed back into the agent's understanding
    """

    plan_id: str = Field(description="Unique plan ID")
    created_at: datetime = Field(default_factory=datetime.now)
    trigger_summary: str = Field(description="What prompted this plan")
    decisions: list[Decision] = Field(default_factory=list)
    auto_execute: bool = Field(
        default=False,
        description="Whether to auto-execute or wait for approval",
    )

    # Aggregate metrics
    total_targets: int = 0
    estimated_improvement: float = Field(
        ge=0.0, le=1.0, default=0.0,
        description="Estimated health score improvement after execution",
    )

    def critical_decisions(self) -> list[Decision]:
        return [d for d in self.decisions if d.severity == Severity.CRITICAL]

    def auto_executable_decisions(self) -> list[Decision]:
        """Decisions safe to auto-execute without human review."""
        safe_types = {
            DecisionType.DELETE_DUPLICATES,
            DecisionType.DELETE_EMPTY_CHUNKS,
            DecisionType.ENRICH_METADATA,
            DecisionType.NORMALIZE_PATHS,
            DecisionType.NO_ACTION,
        }
        return [d for d in self.decisions if d.decision_type in safe_types]

    def needs_approval_decisions(self) -> list[Decision]:
        """Decisions that should be reviewed before execution."""
        safe = set(d.id for d in self.auto_executable_decisions())
        return [d for d in self.decisions if d.id not in safe]

    def to_structured_output(self) -> dict[str, Any]:
        """Machine-readable plan output for agent consumption."""
        return {
            "plan_id": self.plan_id,
            "trigger": self.trigger_summary,
            "created_at": self.created_at.isoformat(),
            "total_decisions": len(self.decisions),
            "total_targets": self.total_targets,
            "estimated_improvement": self.estimated_improvement,
            "auto_executable": len(self.auto_executable_decisions()),
            "needs_approval": len(self.needs_approval_decisions()),
            "decisions": [
                {
                    "id": d.id,
                    "type": d.decision_type.value,
                    "severity": d.severity.value,
                    "confidence": d.confidence,
                    "target_count": len(d.target_ids),
                    "reasoning": d.reasoning,
                    "expected_impact": d.expected_impact,
                    "executed": d.executed,
                }
                for d in self.decisions
            ],
            "summary": {
                "critical": len([d for d in self.decisions if d.severity == Severity.CRITICAL]),
                "high": len([d for d in self.decisions if d.severity == Severity.HIGH]),
                "medium": len([d for d in self.decisions if d.severity == Severity.MEDIUM]),
                "low": len([d for d in self.decisions if d.severity == Severity.LOW]),
            },
        }
