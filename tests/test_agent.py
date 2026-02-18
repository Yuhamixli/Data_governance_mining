"""Tests for the autonomous GovernanceAgent."""

import json
import tempfile
from pathlib import Path

import pytest

from data_governance.agent.decisions import (
    ActionPlan,
    Decision,
    DecisionType,
    Severity,
)
from data_governance.agent.governance_agent import GovernanceAgent


class TestDecisions:
    def test_decision_to_agent_message(self):
        d = Decision(
            id="test_1",
            decision_type=DecisionType.DELETE_DUPLICATES,
            severity=Severity.HIGH,
            confidence=0.95,
            trigger="Found 10 duplicates",
            reasoning="Duplicates degrade search quality",
        )
        msg = d.to_agent_message()
        assert "HIGH" in msg
        assert "delete_duplicates" in msg
        assert "95%" in msg

    def test_action_plan_auto_executable(self):
        plan = ActionPlan(
            plan_id="test_plan",
            trigger_summary="Test",
            decisions=[
                Decision(
                    id="d1",
                    decision_type=DecisionType.DELETE_DUPLICATES,
                    severity=Severity.HIGH,
                    confidence=0.99,
                    trigger="test",
                ),
                Decision(
                    id="d2",
                    decision_type=DecisionType.DELETE_EXPIRED,
                    severity=Severity.MEDIUM,
                    confidence=0.90,
                    trigger="test",
                ),
                Decision(
                    id="d3",
                    decision_type=DecisionType.NO_ACTION,
                    severity=Severity.LOW,
                    confidence=1.0,
                    trigger="test",
                ),
            ],
        )

        auto = plan.auto_executable_decisions()
        needs_approval = plan.needs_approval_decisions()

        # DELETE_DUPLICATES and NO_ACTION are safe; DELETE_EXPIRED needs approval
        assert len(auto) == 2
        assert len(needs_approval) == 1
        assert needs_approval[0].decision_type == DecisionType.DELETE_EXPIRED

    def test_plan_structured_output(self):
        plan = ActionPlan(
            plan_id="test_plan",
            trigger_summary="Test trigger",
            decisions=[
                Decision(
                    id="d1",
                    decision_type=DecisionType.DELETE_DUPLICATES,
                    severity=Severity.HIGH,
                    confidence=0.99,
                    trigger="test",
                    target_ids=["a", "b"],
                    reasoning="Testing",
                    expected_impact="Improve quality",
                ),
            ],
            total_targets=2,
        )

        output = plan.to_structured_output()
        assert output["plan_id"] == "test_plan"
        assert output["total_decisions"] == 1
        assert output["total_targets"] == 2
        assert output["decisions"][0]["type"] == "delete_duplicates"
        assert output["decisions"][0]["confidence"] == 0.99

    def test_plan_severity_summary(self):
        plan = ActionPlan(
            plan_id="test",
            trigger_summary="Test",
            decisions=[
                Decision(id="1", decision_type=DecisionType.QUARANTINE, severity=Severity.CRITICAL, confidence=0.9, trigger="t"),
                Decision(id="2", decision_type=DecisionType.DELETE_DUPLICATES, severity=Severity.HIGH, confidence=0.9, trigger="t"),
                Decision(id="3", decision_type=DecisionType.SCHEDULE_REVIEW, severity=Severity.LOW, confidence=0.8, trigger="t"),
            ],
        )
        output = plan.to_structured_output()
        assert output["summary"]["critical"] == 1
        assert output["summary"]["high"] == 1
        assert output["summary"]["low"] == 1


class TestGovernanceAgent:
    def test_agent_init(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            agent = GovernanceAgent(
                workspace_path=tmpdir,
                memory_path=Path(tmpdir) / ".gov" / "memory.json",
            )
            assert agent.memory == {}

    def test_should_run_first_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            agent = GovernanceAgent(
                workspace_path=tmpdir,
                memory_path=Path(tmpdir) / ".gov" / "memory.json",
            )
            assert agent.should_run() is True

    def test_governance_state_structure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal workspace
            knowledge = Path(tmpdir) / "knowledge" / "长期"
            knowledge.mkdir(parents=True)
            (knowledge / "test.md").write_text("test content", encoding="utf-8")

            agent = GovernanceAgent(
                workspace_path=tmpdir,
                memory_path=Path(tmpdir) / ".gov" / "memory.json",
            )

            state = agent.get_governance_state()

            # Verify structured state for agent consumption
            assert "timestamp" in state
            assert "health" in state
            assert "dimensions" in state
            assert "actionable" in state
            assert isinstance(state["actionable"], dict)
            assert "needs_dedup" in state["actionable"]

    def test_compute_trend(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            agent = GovernanceAgent(
                workspace_path=tmpdir,
                memory_path=Path(tmpdir) / ".gov" / "memory.json",
            )

            # No history
            assert agent._compute_trend() == "unknown"

            # Add improving history
            agent.memory["cycles"] = [
                {"health_score": 0.5, "timestamp": "2026-01-01"},
                {"health_score": 0.6, "timestamp": "2026-01-02"},
                {"health_score": 0.7, "timestamp": "2026-01-03"},
            ]
            assert agent._compute_trend() == "improving"

            # Add degrading history
            agent.memory["cycles"] = [
                {"health_score": 0.8, "timestamp": "2026-01-01"},
                {"health_score": 0.7, "timestamp": "2026-01-02"},
                {"health_score": 0.6, "timestamp": "2026-01-03"},
            ]
            assert agent._compute_trend() == "degrading"
