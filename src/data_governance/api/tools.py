"""Agent tool definitions — designed for AI agent consumption, not human use.

Philosophy: Every tool output is structured JSON that an agent can parse,
reason about, and act on. No pretty-printing, no markdown — pure machine
signal. The agent decides how to present information to humans if needed.

These tools follow the nanobot tool pattern: name, description, parameters
schema, and an execute function. The GovernanceToolkit generates tool
definitions that can be directly registered with nanobot's tool registry.
"""

from __future__ import annotations

import json
from typing import Any

from data_governance.api.facade import GovernanceFacade
from data_governance.agent.governance_agent import GovernanceAgent
from data_governance.protocol.quality_embed import QualityEmbedder
from data_governance.daemon.monitor import GovernanceDaemon


# Tool definitions for nanobot's tool registry
TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "governance_agent_cycle",
        "description": (
            "Run a full autonomous governance cycle: perceive data quality signals, "
            "reason about issues, create an action plan, and auto-execute safe "
            "remediations. Returns structured decisions with confidence levels. "
            "This is the PRIMARY governance tool — use it for comprehensive assessment."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "auto_execute": {
                    "type": "boolean",
                    "description": "Auto-execute safe remediations (dedup, empty chunk removal)",
                    "default": True,
                },
            },
            "required": [],
        },
    },
    {
        "name": "governance_state",
        "description": (
            "Get the complete governance state as structured data. "
            "Returns health scores, quality dimensions, actionable signals, "
            "active alerts, and trend information. Use this to understand "
            "the current quality of your knowledge base AT A GLANCE."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "governance_health_check",
        "description": (
            "Run a comprehensive health check on the knowledge base. "
            "Returns overall health score, component scores (quality, validation, "
            "uniqueness, freshness), issues found, and actionable recommendations."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "governance_find_duplicates",
        "description": (
            "Find duplicate chunks in the knowledge base vector store. "
            "Reports exact duplicates (by content hash) and optionally "
            "near-duplicates (by embedding similarity)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "include_semantic": {
                    "type": "boolean",
                    "description": "Also detect near-duplicates by embedding similarity",
                    "default": False,
                },
            },
            "required": [],
        },
    },
    {
        "name": "governance_remove_duplicates",
        "description": (
            "Remove duplicate chunks from the knowledge base vector store. "
            "DESTRUCTIVE operation — removes redundant chunks while "
            "keeping one copy of each unique content."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "include_semantic": {
                    "type": "boolean",
                    "description": "Also remove near-duplicates by embedding similarity",
                    "default": False,
                },
            },
            "required": [],
        },
    },
    {
        "name": "governance_validate",
        "description": (
            "Run validation checks on knowledge base data. "
            "Checks for empty chunks, encoding issues, missing metadata, "
            "and other quality problems. Returns structured validation results."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "target": {
                    "type": "string",
                    "description": "What to validate: knowledge_base, documents, chat_history, memory, all",
                    "enum": ["knowledge_base", "documents", "chat_history", "memory", "all"],
                    "default": "knowledge_base",
                },
            },
            "required": [],
        },
    },
    {
        "name": "governance_check_freshness",
        "description": (
            "Check data freshness across the knowledge base. "
            "Identifies stale and expired data with recommended actions."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "governance_profile_document",
        "description": (
            "Profile a specific document's quality BEFORE ingestion. "
            "Returns quality gate decision: ingest or reject."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the document file to profile",
                },
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "governance_embed_quality",
        "description": (
            "Embed quality scores into ChromaDB chunk metadata. "
            "After this, every chunk carries quality_score, freshness_score, "
            "and is_quarantined fields — enabling quality-aware retrieval."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "governance_get_alerts",
        "description": (
            "Get active governance alerts that need attention."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "governance_get_lineage",
        "description": (
            "Get data lineage — trace where data came from (upstream) "
            "or what depends on it (downstream)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "asset_id": {
                    "type": "string",
                    "description": "Asset ID (e.g., 'file:doc.md', 'chunk:abc123')",
                },
                "direction": {
                    "type": "string",
                    "description": "upstream (sources) or downstream (consumers)",
                    "enum": ["upstream", "downstream"],
                    "default": "upstream",
                },
            },
            "required": ["asset_id"],
        },
    },
]


class GovernanceToolkit:
    """Agent-callable governance toolkit.

    All outputs are structured JSON — designed for machine reasoning,
    not human reading. The consuming agent decides presentation.

    Usage in nanobot:
        toolkit = GovernanceToolkit(workspace_path=..., chromadb_path=...)

        # Register tools
        for tool_def in toolkit.get_tool_definitions():
            register_tool(tool_def)

        # Execute — returns structured JSON string
        result = toolkit.execute("governance_agent_cycle", {"auto_execute": True})
        data = json.loads(result)  # Agent can parse and reason about this
    """

    def __init__(
        self,
        workspace_path: str | None = None,
        chromadb_path: str | None = None,
        collection_name: str = "nanobot_kb",
    ):
        self.facade = GovernanceFacade(
            workspace_path=workspace_path,
            chromadb_path=chromadb_path,
            collection_name=collection_name,
        )
        self.agent = GovernanceAgent(
            workspace_path=workspace_path,
            chromadb_path=chromadb_path,
            collection_name=collection_name,
        )
        self.quality_embedder = QualityEmbedder(self.facade.config)
        self.daemon = GovernanceDaemon(
            workspace_path=workspace_path,
            chromadb_path=chromadb_path,
            collection_name=collection_name,
        )

    def get_tool_definitions(self) -> list[dict[str, Any]]:
        """Get tool definitions for registration with the agent."""
        return TOOL_DEFINITIONS

    def execute(self, tool_name: str, args: dict[str, Any]) -> str:
        """Execute a governance tool and return structured JSON.

        All outputs are machine-readable JSON strings.
        """
        try:
            handler = self._handlers.get(tool_name)
            if handler is None:
                return _json_output({"error": f"Unknown tool: {tool_name}"})
            return handler(self, args)
        except Exception as e:
            return _json_output({
                "error": str(e),
                "tool": tool_name,
                "status": "failed",
            })

    # ── Handlers ──────────────────────────────────────────────────

    def _agent_cycle(self, args: dict[str, Any]) -> str:
        plan = self.agent.perceive_and_decide()
        result = plan.to_structured_output()

        if args.get("auto_execute", True):
            exec_result = self.agent.execute_plan(plan, auto_only=True)
            result["execution"] = exec_result

        result["governance_state"] = self.agent.get_governance_state()
        return _json_output(result)

    def _governance_state(self, args: dict[str, Any]) -> str:
        return _json_output(self.agent.get_governance_state())

    def _health_check(self, args: dict[str, Any]) -> str:
        health = self.facade.health_check()
        return _json_output(health.to_dict())

    def _find_duplicates(self, args: dict[str, Any]) -> str:
        include_semantic = args.get("include_semantic", False)
        report = self.facade.find_duplicates(include_semantic=include_semantic)
        return _json_output({
            "hash_duplicates": report.hash_result.duplicate_count,
            "hash_unique": report.hash_result.unique_items,
            "hash_total": report.hash_result.total_items,
            "hash_ratio": report.hash_result.duplicate_ratio,
            "semantic_duplicates": (
                report.semantic_result.duplicate_count if report.semantic_result else None
            ),
            "total_duplicates": report.total_duplicates,
            "groups": len(report.hash_result.duplicate_groups),
            "removable_ids": report.hash_result.removed_ids[:50],
        })

    def _remove_duplicates(self, args: dict[str, Any]) -> str:
        include_semantic = args.get("include_semantic", False)
        report = self.facade.remove_duplicates(include_semantic=include_semantic)
        return _json_output({
            "removed": report.hash_result.duplicate_count,
            "remaining": report.hash_result.unique_items,
            "actions": report.actions_taken,
        })

    def _validate(self, args: dict[str, Any]) -> str:
        target = args.get("target", "knowledge_base")
        results = []

        if target in ("all", "knowledge_base"):
            results.extend(self.facade.validate_knowledge_base())
        if target in ("all", "documents"):
            results.extend(self.facade.validate_documents())
        if target in ("all", "chat_history"):
            results.extend(self.facade.validate_chat_history())
        if target in ("all", "memory"):
            results.extend(self.facade.validate_memory())

        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failures_by_rule: dict[str, int] = {}
        for r in results:
            if not r.passed:
                failures_by_rule[r.rule_name] = failures_by_rule.get(r.rule_name, 0) + 1

        return _json_output({
            "total_checks": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": passed / total if total > 0 else 1.0,
            "failures_by_rule": failures_by_rule,
            "failure_details": [
                {
                    "rule": r.rule_name,
                    "severity": r.severity.value,
                    "message": r.message,
                    "asset_id": r.asset_id,
                }
                for r in results
                if not r.passed
            ][:50],
        })

    def _check_freshness(self, args: dict[str, Any]) -> str:
        report = self.facade.check_freshness()
        return _json_output({
            "total_assets": report.total,
            "avg_freshness": report.avg_freshness,
            "stale_count": report.stale_count,
            "expired_count": report.expired_count,
            "stale_assets": [
                {"id": r.asset_id, "name": r.asset_name, "age_days": r.age_days, "score": r.freshness_score}
                for r in report.records if r.is_stale
            ][:20],
            "expired_assets": [
                {"id": r.asset_id, "name": r.asset_name, "path": r.source_path}
                for r in report.records if r.is_expired
            ][:20],
        })

    def _profile_document(self, args: dict[str, Any]) -> str:
        file_path = args.get("file_path", "")
        if not file_path:
            return _json_output({"error": "file_path is required"})
        report = self.facade.profile_document(file_path)
        return _json_output({
            "file": file_path,
            "overall_score": report.overall_score,
            "dimensions": {
                s.dimension.value: {"score": s.score, "details": s.details}
                for s in report.dimension_scores
            },
            "gate_decision": "ingest" if report.overall_score >= 0.3 else "reject",
            "issues": report.issues_found,
            "recommendations": report.recommendations,
        })

    def _embed_quality(self, args: dict[str, Any]) -> str:
        if not self.facade.chromadb_path:
            return _json_output({"error": "No ChromaDB path configured"})
        result = self.quality_embedder.embed_quality_scores(
            self.facade.chromadb_path, self.facade.collection_name
        )
        return _json_output(result)

    def _get_alerts(self, args: dict[str, Any]) -> str:
        alerts = self.facade.get_alerts()
        return _json_output({
            "active_count": len(alerts),
            "alerts": alerts,
        })

    def _get_lineage(self, args: dict[str, Any]) -> str:
        asset_id = args.get("asset_id", "")
        direction = args.get("direction", "upstream")
        if not asset_id:
            return _json_output({"error": "asset_id is required"})
        nodes = self.facade.get_lineage(asset_id, direction)
        return _json_output({
            "asset_id": asset_id,
            "direction": direction,
            "lineage": nodes,
        })

    # Handler dispatch table
    _handlers = {
        "governance_agent_cycle": _agent_cycle,
        "governance_state": _governance_state,
        "governance_health_check": _health_check,
        "governance_find_duplicates": _find_duplicates,
        "governance_remove_duplicates": _remove_duplicates,
        "governance_validate": _validate,
        "governance_check_freshness": _check_freshness,
        "governance_profile_document": _profile_document,
        "governance_embed_quality": _embed_quality,
        "governance_get_alerts": _get_alerts,
        "governance_get_lineage": _get_lineage,
    }


def _json_output(data: dict[str, Any]) -> str:
    """Serialize output as compact JSON for machine consumption."""
    return json.dumps(data, ensure_ascii=False, default=str)
