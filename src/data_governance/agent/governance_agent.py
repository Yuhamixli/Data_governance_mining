"""GovernanceAgent — an autonomous, self-operating data governance agent.

This is NOT a toolkit for humans. This is an AI agent that:
1. Continuously monitors data quality
2. Reasons about what's wrong and why
3. Makes decisions about what to fix
4. Executes remediation autonomously
5. Learns from outcomes to improve future decisions

Design philosophy (from Matt Shumer's insight):
- We are not programming for humans, we are programming for AI
- The governance agent IS the consumer of governance data
- Every output is structured for machine reasoning, not human reading
- The agent should be able to operate without human intervention
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from data_governance.agent.decisions import (
    ActionPlan,
    Decision,
    DecisionType,
    Severity,
)
from data_governance.api.facade import GovernanceFacade
from data_governance.core.config import GovernanceConfig


class GovernanceAgent:
    """Autonomous data governance agent.

    Unlike GovernanceFacade (which is a tool library), GovernanceAgent
    is a decision-making entity that:

    - Perceives: scans data quality signals
    - Reasons: analyzes patterns, identifies root causes
    - Decides: creates action plans with confidence levels
    - Acts: executes safe remediations autonomously
    - Remembers: persists decisions and outcomes for learning

    Usage:
        agent = GovernanceAgent(
            workspace_path="~/.xnobot/workspace",
            chromadb_path="~/.xnobot/workspace/knowledge_db",
        )

        # Full autonomous cycle
        plan = agent.perceive_and_decide()

        # Execute safe actions automatically
        results = agent.execute_plan(plan, auto_only=True)

        # Get structured state for another agent to consume
        state = agent.get_governance_state()
    """

    def __init__(
        self,
        workspace_path: str | Path | None = None,
        chromadb_path: str | Path | None = None,
        collection_name: str = "xnobot_kb",
        config: GovernanceConfig | None = None,
        memory_path: str | Path | None = None,
    ):
        self.facade = GovernanceFacade(
            workspace_path=workspace_path,
            chromadb_path=chromadb_path,
            collection_name=collection_name,
            config=config,
        )
        self.memory_path = Path(memory_path) if memory_path else (
            Path(workspace_path or ".") / ".governance" / "agent_memory.json"
        )
        self.memory = self._load_memory()

    # ── Core Agent Loop ───────────────────────────────────────────

    def perceive_and_decide(self) -> ActionPlan:
        """Full agent cycle: perceive data state → reason → generate action plan.

        This is the primary entry point. Call this and the agent will:
        1. Scan all data quality signals
        2. Analyze patterns and anomalies
        3. Generate a prioritized action plan
        4. Return the plan for execution
        """
        # Phase 1: Perceive — gather all signals
        signals = self._perceive()

        # Phase 2: Reason — analyze signals, identify issues
        decisions = self._reason(signals)

        # Phase 3: Plan — organize decisions into an executable plan
        plan = self._create_plan(decisions, signals)

        # Phase 4: Remember — record this perception cycle
        self._remember_cycle(signals, plan)

        return plan

    def execute_plan(
        self, plan: ActionPlan, auto_only: bool = True
    ) -> dict[str, Any]:
        """Execute an action plan.

        Args:
            plan: The action plan to execute.
            auto_only: If True, only execute safe auto-executable actions.
                      If False, execute ALL actions (use with caution).
        """
        decisions_to_execute = (
            plan.auto_executable_decisions() if auto_only else plan.decisions
        )

        results: dict[str, Any] = {
            "plan_id": plan.plan_id,
            "executed": 0,
            "skipped": 0,
            "failed": 0,
            "actions": [],
        }

        for decision in decisions_to_execute:
            try:
                result = self._execute_decision(decision)
                decision.executed = True
                decision.execution_result = result
                results["executed"] += 1
                results["actions"].append({
                    "decision_id": decision.id,
                    "type": decision.decision_type.value,
                    "success": True,
                    "result": result,
                })
            except Exception as e:
                results["failed"] += 1
                results["actions"].append({
                    "decision_id": decision.id,
                    "type": decision.decision_type.value,
                    "success": False,
                    "error": str(e),
                })

        results["skipped"] = len(plan.decisions) - len(decisions_to_execute)
        self._remember_execution(plan, results)
        return results

    def get_governance_state(self) -> dict[str, Any]:
        """Get the complete governance state as structured data.

        This is designed for consumption by OTHER AI agents.
        Not a report — a machine-readable state snapshot that another
        agent can reason about, query, and act on.
        """
        signals = self._perceive()

        state = {
            "timestamp": datetime.now().isoformat(),
            "version": "0.1.0",
            "workspace": str(self.facade.workspace_path),

            # Overall health signal (0-1, higher is better)
            "health": {
                "overall": signals.get("health_score", 0),
                "status": _health_status(signals.get("health_score", 0)),
                "trend": self._compute_trend(),
            },

            # Per-dimension signals
            "dimensions": {
                "quality": {
                    "score": signals.get("quality_score", 0),
                    "total_items": signals.get("total_chunks", 0),
                    "issues": signals.get("quality_issues", 0),
                },
                "uniqueness": {
                    "score": 1.0 - signals.get("duplicate_ratio", 0),
                    "duplicate_count": signals.get("duplicate_count", 0),
                },
                "freshness": {
                    "score": signals.get("freshness_score", 0),
                    "stale_count": signals.get("stale_count", 0),
                    "expired_count": signals.get("expired_count", 0),
                },
                "validity": {
                    "score": signals.get("validation_pass_rate", 0),
                    "error_count": signals.get("validation_errors", 0),
                },
            },

            # Actionable signals for other agents
            "actionable": {
                "needs_dedup": signals.get("duplicate_count", 0) > 0,
                "needs_cleanup": signals.get("expired_count", 0) > 0,
                "needs_review": signals.get("stale_count", 0) > 5,
                "needs_repair": signals.get("validation_errors", 0) > 0,
            },

            # Active alerts
            "alerts": signals.get("alerts", []),

            # Agent memory summary
            "agent_cycles": len(self.memory.get("cycles", [])),
            "last_action": self.memory.get("last_action_time"),
        }

        return state

    def should_run(self) -> bool:
        """Determine if the agent should run a governance cycle.

        Uses memory to avoid running too frequently while ensuring
        timely response to degradation.
        """
        last_run = self.memory.get("last_run_time")
        if not last_run:
            return True

        try:
            last_dt = datetime.fromisoformat(last_run)
            hours_since = (datetime.now() - last_dt).total_seconds() / 3600

            # Run at least every 6 hours
            if hours_since >= 6:
                return True

            # Run sooner if health was bad last time
            last_health = self.memory.get("last_health_score", 1.0)
            if last_health < 0.6 and hours_since >= 1:
                return True

            return False
        except (ValueError, TypeError):
            return True

    # ── Perception Layer ──────────────────────────────────────────

    def _perceive(self) -> dict[str, Any]:
        """Gather all data quality signals into a unified signal dict."""
        signals: dict[str, Any] = {}

        # Health check
        try:
            health = self.facade.health_check()
            signals["health_score"] = health.overall
            signals["health_components"] = {
                c.name: c.score for c in health.components
            }
        except Exception:
            signals["health_score"] = 0.0

        # Quality profiling
        try:
            quality = self.facade.profile_knowledge_base()
            signals["quality_score"] = quality.overall_score
            signals["quality_issues"] = quality.issues_found
            signals["total_chunks"] = quality.total_items
        except Exception:
            pass

        # Dedup scan
        try:
            dedup = self.facade.find_duplicates()
            signals["duplicate_count"] = dedup.hash_result.duplicate_count
            signals["duplicate_ratio"] = dedup.hash_result.duplicate_ratio
            signals["duplicate_groups"] = len(dedup.hash_result.duplicate_groups)
            signals["duplicate_ids"] = dedup.hash_result.removed_ids[:100]
        except Exception:
            signals["duplicate_count"] = 0
            signals["duplicate_ratio"] = 0

        # Validation
        try:
            val_results = self.facade.validate_knowledge_base()
            total = len(val_results)
            passed = sum(1 for r in val_results if r.passed)
            signals["validation_total"] = total
            signals["validation_passed"] = passed
            signals["validation_pass_rate"] = passed / total if total > 0 else 1.0
            signals["validation_errors"] = sum(
                1 for r in val_results
                if not r.passed and r.severity.value == "error"
            )
        except Exception:
            signals["validation_pass_rate"] = 1.0
            signals["validation_errors"] = 0

        # Freshness
        try:
            freshness = self.facade.check_freshness()
            signals["freshness_score"] = freshness.avg_freshness
            signals["stale_count"] = freshness.stale_count
            signals["expired_count"] = freshness.expired_count
            signals["stale_assets"] = [
                {"id": r.asset_id, "name": r.asset_name, "age_days": r.age_days}
                for r in freshness.records if r.is_stale
            ][:20]
            signals["expired_assets"] = [
                {"id": r.asset_id, "name": r.asset_name, "path": r.source_path}
                for r in freshness.records if r.is_expired
            ][:20]
        except Exception:
            signals["freshness_score"] = 1.0
            signals["stale_count"] = 0
            signals["expired_count"] = 0

        # Alerts
        try:
            signals["alerts"] = self.facade.get_alerts()
        except Exception:
            signals["alerts"] = []

        return signals

    # ── Reasoning Layer ───────────────────────────────────────────

    def _reason(self, signals: dict[str, Any]) -> list[Decision]:
        """Analyze signals and generate governance decisions.

        This is where the agent "thinks" — it doesn't just threshold-check,
        it considers context, severity, dependencies, and confidence.
        """
        decisions: list[Decision] = []

        # Rule 1: Duplicates should always be removed
        dup_count = signals.get("duplicate_count", 0)
        if dup_count > 0:
            dup_ids = signals.get("duplicate_ids", [])
            decisions.append(Decision(
                id=f"dedup_{_short_id()}",
                decision_type=DecisionType.DELETE_DUPLICATES,
                severity=Severity.HIGH if dup_count > 10 else Severity.MEDIUM,
                confidence=0.99,
                trigger=f"Found {dup_count} exact duplicate chunks",
                evidence={"duplicate_count": dup_count, "groups": signals.get("duplicate_groups", 0)},
                target_ids=dup_ids,
                reasoning=(
                    f"{dup_count} exact-duplicate chunks detected. Duplicates degrade search "
                    f"relevance by diluting result diversity. Each duplicate occupies embedding "
                    f"space without adding information."
                ),
                expected_impact=f"Remove {dup_count} redundant chunks, improve search precision",
            ))

        # Rule 2: Empty chunks are always noise
        quality_issues = signals.get("quality_issues", 0)
        if quality_issues > 0 and signals.get("quality_score", 1) < 0.7:
            decisions.append(Decision(
                id=f"quality_{_short_id()}",
                decision_type=DecisionType.DELETE_EMPTY_CHUNKS,
                severity=Severity.HIGH,
                confidence=0.95,
                trigger=f"Quality score {signals.get('quality_score', 0):.1%} with {quality_issues} issues",
                evidence={"quality_score": signals.get("quality_score"), "issues": quality_issues},
                target_ids=[],  # Will be populated during execution
                reasoning=(
                    "Low quality score indicates significant presence of empty, corrupted, "
                    "or sub-threshold chunks. These add noise to retrieval and waste embedding space."
                ),
                expected_impact="Remove noise chunks, improve retrieval signal-to-noise ratio",
            ))

        # Rule 3: Expired data should be cleaned
        expired_count = signals.get("expired_count", 0)
        if expired_count > 0:
            expired_assets = signals.get("expired_assets", [])
            decisions.append(Decision(
                id=f"expire_{_short_id()}",
                decision_type=DecisionType.DELETE_EXPIRED,
                severity=Severity.MEDIUM,
                confidence=0.90,
                trigger=f"{expired_count} expired assets detected",
                evidence={"expired_count": expired_count, "assets": expired_assets},
                target_ids=[a["id"] for a in expired_assets],
                reasoning=(
                    f"{expired_count} data assets have exceeded their TTL. "
                    f"Expired data may contain outdated information that could lead "
                    f"the agent to generate incorrect responses."
                ),
                expected_impact="Remove outdated data, improve response accuracy",
            ))

        # Rule 4: Stale data should be flagged for review
        stale_count = signals.get("stale_count", 0)
        if stale_count > 5:
            decisions.append(Decision(
                id=f"stale_{_short_id()}",
                decision_type=DecisionType.SCHEDULE_REVIEW,
                severity=Severity.LOW,
                confidence=0.80,
                trigger=f"{stale_count} stale assets detected",
                evidence={"stale_count": stale_count, "assets": signals.get("stale_assets", [])},
                target_ids=[a["id"] for a in signals.get("stale_assets", [])],
                reasoning=(
                    f"{stale_count} assets are approaching staleness. Not yet expired, "
                    f"but their information may be becoming outdated."
                ),
                expected_impact="Proactive maintenance to prevent data quality drift",
            ))

        # Rule 5: Validation errors indicate data corruption
        val_errors = signals.get("validation_errors", 0)
        if val_errors > 0:
            decisions.append(Decision(
                id=f"validate_{_short_id()}",
                decision_type=DecisionType.QUARANTINE,
                severity=Severity.HIGH if val_errors > 10 else Severity.MEDIUM,
                confidence=0.85,
                trigger=f"{val_errors} validation errors detected",
                evidence={"error_count": val_errors, "pass_rate": signals.get("validation_pass_rate")},
                target_ids=[],
                reasoning=(
                    f"{val_errors} chunks failed validation (encoding errors, missing metadata, "
                    f"control characters). These corrupt chunks could introduce garbled content "
                    f"into agent responses."
                ),
                expected_impact="Isolate corrupt data to prevent contamination of responses",
            ))

        # Rule 6: If everything is healthy, say so
        if not decisions:
            health = signals.get("health_score", 0)
            decisions.append(Decision(
                id=f"ok_{_short_id()}",
                decision_type=DecisionType.NO_ACTION,
                severity=Severity.LOW,
                confidence=1.0,
                trigger="Routine governance check",
                evidence={"health_score": health},
                reasoning=f"Knowledge base health is {health:.1%}. No governance actions needed.",
                expected_impact="No change",
            ))

        return decisions

    # ── Planning Layer ────────────────────────────────────────────

    def _create_plan(
        self, decisions: list[Decision], signals: dict[str, Any]
    ) -> ActionPlan:
        """Organize decisions into a prioritized, executable plan."""
        # Sort by severity (critical first) then confidence
        severity_order = {Severity.CRITICAL: 0, Severity.HIGH: 1, Severity.MEDIUM: 2, Severity.LOW: 3}
        decisions.sort(key=lambda d: (severity_order.get(d.severity, 3), -d.confidence))

        total_targets = sum(len(d.target_ids) for d in decisions)

        # Estimate improvement
        current_health = signals.get("health_score", 0.5)
        actionable = [d for d in decisions if d.decision_type != DecisionType.NO_ACTION]
        estimated_improvement = min(
            0.3, len(actionable) * 0.05
        ) if actionable else 0.0

        plan = ActionPlan(
            plan_id=f"plan_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_short_id()}",
            trigger_summary=f"Governance cycle at health={current_health:.1%}",
            decisions=decisions,
            auto_execute=current_health < 0.5,  # Auto-execute when health is critical
            total_targets=total_targets,
            estimated_improvement=estimated_improvement,
        )

        return plan

    # ── Execution Layer ───────────────────────────────────────────

    def _execute_decision(self, decision: Decision) -> dict[str, Any]:
        """Execute a single governance decision."""
        handlers = {
            DecisionType.DELETE_DUPLICATES: self._exec_delete_duplicates,
            DecisionType.DELETE_EMPTY_CHUNKS: self._exec_delete_empty_chunks,
            DecisionType.DELETE_EXPIRED: self._exec_delete_expired,
            DecisionType.ENRICH_METADATA: self._exec_enrich_metadata,
            DecisionType.QUARANTINE: self._exec_quarantine,
            DecisionType.NO_ACTION: lambda d: {"action": "none"},
        }
        handler = handlers.get(decision.decision_type)
        if handler:
            return handler(decision)
        return {"action": "unsupported", "type": decision.decision_type.value}

    def _exec_delete_duplicates(self, decision: Decision) -> dict[str, Any]:
        """Remove exact duplicate chunks from the vector store."""
        report = self.facade.remove_duplicates(include_semantic=False)
        return {
            "action": "delete_duplicates",
            "removed": report.hash_result.duplicate_count,
            "remaining": report.hash_result.unique_items,
        }

    def _exec_delete_empty_chunks(self, decision: Decision) -> dict[str, Any]:
        """Remove empty and gibberish chunks."""
        if not self.facade.chromadb_path:
            return {"action": "skip", "reason": "no chromadb configured"}

        import chromadb
        client = chromadb.PersistentClient(path=self.facade.chromadb_path)
        collection = client.get_collection(self.facade.collection_name)
        data = collection.get(include=["documents"])

        ids_to_remove = []
        for doc_id, content in zip(data["ids"], data["documents"]):
            if not content or not content.strip():
                ids_to_remove.append(doc_id)
            elif len(content.strip()) < 10:
                ids_to_remove.append(doc_id)

        if ids_to_remove:
            for i in range(0, len(ids_to_remove), 100):
                batch = ids_to_remove[i:i + 100]
                collection.delete(ids=batch)

        return {"action": "delete_empty", "removed": len(ids_to_remove)}

    def _exec_delete_expired(self, decision: Decision) -> dict[str, Any]:
        """Delete expired files and their vector entries."""
        expired = self.facade.get_expired_assets()
        deleted_files = 0
        for asset in expired:
            path = asset.get("path")
            if path and Path(path).exists():
                action = asset.get("action", "review")
                if action == "delete":
                    Path(path).unlink()
                    deleted_files += 1
        return {"action": "delete_expired", "deleted_files": deleted_files}

    def _exec_enrich_metadata(self, decision: Decision) -> dict[str, Any]:
        """Enrich chunk metadata with quality scores."""
        # Implemented by quality_embedder module
        return {"action": "enrich_metadata", "status": "delegated"}

    def _exec_quarantine(self, decision: Decision) -> dict[str, Any]:
        """Quarantine problematic chunks by tagging them."""
        if not self.facade.chromadb_path:
            return {"action": "skip", "reason": "no chromadb configured"}

        import chromadb
        client = chromadb.PersistentClient(path=self.facade.chromadb_path)
        collection = client.get_collection(self.facade.collection_name)

        # Find problematic chunks
        problems = self.facade.chunk_profiler.find_problematic_chunks(
            *self._get_chunks_from_collection(collection)
        )

        quarantined_ids = []
        for category in ["empty", "gibberish"]:
            for item in problems.get(category, []):
                quarantined_ids.append(item["id"])

        # Remove quarantined chunks
        if quarantined_ids:
            for i in range(0, len(quarantined_ids), 100):
                batch = quarantined_ids[i:i + 100]
                collection.delete(ids=batch)

        return {"action": "quarantine", "quarantined": len(quarantined_ids)}

    def _get_chunks_from_collection(self, collection) -> tuple[list[str], list[str]]:
        """Helper to get chunks and IDs from a ChromaDB collection."""
        data = collection.get(include=["documents"])
        return data.get("documents", []), data.get("ids", [])

    # ── Memory Layer ──────────────────────────────────────────────

    def _remember_cycle(self, signals: dict[str, Any], plan: ActionPlan) -> None:
        """Persist governance cycle to agent memory."""
        cycle = {
            "timestamp": datetime.now().isoformat(),
            "health_score": signals.get("health_score", 0),
            "plan_id": plan.plan_id,
            "decisions": len(plan.decisions),
            "auto_executable": len(plan.auto_executable_decisions()),
        }

        if "cycles" not in self.memory:
            self.memory["cycles"] = []
        self.memory["cycles"].append(cycle)

        # Keep last 100 cycles
        self.memory["cycles"] = self.memory["cycles"][-100:]
        self.memory["last_run_time"] = datetime.now().isoformat()
        self.memory["last_health_score"] = signals.get("health_score", 0)

        self._save_memory()

    def _remember_execution(self, plan: ActionPlan, results: dict[str, Any]) -> None:
        """Record execution results for learning."""
        execution = {
            "timestamp": datetime.now().isoformat(),
            "plan_id": plan.plan_id,
            "executed": results["executed"],
            "failed": results["failed"],
            "skipped": results["skipped"],
        }

        if "executions" not in self.memory:
            self.memory["executions"] = []
        self.memory["executions"].append(execution)
        self.memory["executions"] = self.memory["executions"][-100:]
        self.memory["last_action_time"] = datetime.now().isoformat()

        self._save_memory()

    def _compute_trend(self) -> str:
        """Compute health trend from memory."""
        cycles = self.memory.get("cycles", [])
        if len(cycles) < 2:
            return "unknown"

        recent = [c["health_score"] for c in cycles[-5:]]
        if len(recent) < 2:
            return "unknown"

        delta = recent[-1] - recent[0]
        if delta > 0.05:
            return "improving"
        elif delta < -0.05:
            return "degrading"
        return "stable"

    def _load_memory(self) -> dict[str, Any]:
        """Load agent memory from disk."""
        if self.memory_path.exists():
            try:
                return json.loads(self.memory_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return {}
        return {}

    def _save_memory(self) -> None:
        """Save agent memory to disk."""
        self.memory_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.memory_path, "w", encoding="utf-8") as f:
            json.dump(self.memory, f, indent=2, ensure_ascii=False, default=str)


def _short_id() -> str:
    return uuid.uuid4().hex[:8]


def _health_status(score: float) -> str:
    if score >= 0.8:
        return "healthy"
    elif score >= 0.6:
        return "degraded"
    elif score >= 0.4:
        return "unhealthy"
    return "critical"
