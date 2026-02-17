"""Agent tool definitions — these are designed to be registered as tools in nanobot.

Each tool follows the nanobot tool pattern: name, description, parameters schema,
and an execute function. The GovernanceToolkit generates tool definitions that
can be directly registered with nanobot's tool registry.
"""

from __future__ import annotations

from typing import Any

from data_governance.api.facade import GovernanceFacade


# Tool schema definitions following nanobot's JSON schema pattern
TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "governance_health_check",
        "description": (
            "Run a comprehensive health check on the knowledge base. "
            "Returns overall health score, component scores (quality, validation, "
            "uniqueness, freshness), issues found, and actionable recommendations. "
            "Use this to assess the overall quality of the knowledge base."
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
            "near-duplicates (by embedding similarity). Use before cleanup."
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
            "This is a DESTRUCTIVE operation — removes redundant chunks while "
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
            "and other quality problems. Returns detailed validation results."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "target": {
                    "type": "string",
                    "description": "What to validate: knowledge_base, documents, chat_history, memory",
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
            "Identifies stale and expired data that should be reviewed, "
            "archived, or deleted."
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
            "Profile a specific document's quality before or after ingestion. "
            "Returns completeness, validity, and freshness scores with recommendations."
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
        "name": "governance_get_alerts",
        "description": (
            "Get active governance alerts. "
            "Shows unresolved issues that need attention, "
            "such as low quality scores, high duplicate ratios, etc."
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
            "Get data lineage for an asset — trace where data came from "
            "(upstream) or what depends on it (downstream)."
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
                    "description": "Lineage direction: upstream or downstream",
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

    Wraps the GovernanceFacade with a tool-oriented interface that can be
    registered with nanobot's tool registry.

    Usage in nanobot:
        from data_governance.api.tools import GovernanceToolkit

        toolkit = GovernanceToolkit(
            workspace_path=workspace_path,
            chromadb_path=chromadb_path,
        )

        # Register tools
        for tool_def in toolkit.get_tool_definitions():
            register_tool(tool_def)

        # Execute a tool
        result = toolkit.execute("governance_health_check", {})
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

    def get_tool_definitions(self) -> list[dict[str, Any]]:
        """Get tool definitions for registration with the agent."""
        return TOOL_DEFINITIONS

    def execute(self, tool_name: str, args: dict[str, Any]) -> str:
        """Execute a governance tool and return a text result.

        Args:
            tool_name: Name of the tool to execute.
            args: Tool arguments.
        """
        try:
            handler = self._get_handler(tool_name)
            if handler is None:
                return f"Unknown governance tool: {tool_name}"
            return handler(args)
        except Exception as e:
            return f"Governance tool error ({tool_name}): {e}"

    def _get_handler(self, tool_name: str):
        handlers = {
            "governance_health_check": self._health_check,
            "governance_find_duplicates": self._find_duplicates,
            "governance_remove_duplicates": self._remove_duplicates,
            "governance_validate": self._validate,
            "governance_check_freshness": self._check_freshness,
            "governance_profile_document": self._profile_document,
            "governance_get_alerts": self._get_alerts,
            "governance_get_lineage": self._get_lineage,
        }
        return handlers.get(tool_name)

    def _health_check(self, args: dict[str, Any]) -> str:
        health = self.facade.health_check()
        return health.to_markdown()

    def _find_duplicates(self, args: dict[str, Any]) -> str:
        include_semantic = args.get("include_semantic", False)
        report = self.facade.find_duplicates(include_semantic=include_semantic)
        return report.summary()

    def _remove_duplicates(self, args: dict[str, Any]) -> str:
        include_semantic = args.get("include_semantic", False)
        report = self.facade.remove_duplicates(include_semantic=include_semantic)
        return report.summary()

    def _validate(self, args: dict[str, Any]) -> str:
        target = args.get("target", "knowledge_base")

        if target == "all":
            results = []
            results.extend(self.facade.validate_knowledge_base())
            results.extend(self.facade.validate_documents())
            results.extend(self.facade.validate_chat_history())
            results.extend(self.facade.validate_memory())
        elif target == "knowledge_base":
            results = self.facade.validate_knowledge_base()
        elif target == "documents":
            results = self.facade.validate_documents()
        elif target == "chat_history":
            results = self.facade.validate_chat_history()
        elif target == "memory":
            results = self.facade.validate_memory()
        else:
            return f"Unknown validation target: {target}"

        return self.facade.validation_engine.summary(results)

    def _check_freshness(self, args: dict[str, Any]) -> str:
        report = self.facade.check_freshness()
        return report.summary()

    def _profile_document(self, args: dict[str, Any]) -> str:
        file_path = args.get("file_path", "")
        if not file_path:
            return "Error: file_path is required"
        report = self.facade.profile_document(file_path)
        return report.to_summary()

    def _get_alerts(self, args: dict[str, Any]) -> str:
        return self.facade.alert_manager.summary()

    def _get_lineage(self, args: dict[str, Any]) -> str:
        asset_id = args.get("asset_id", "")
        direction = args.get("direction", "upstream")
        if not asset_id:
            return "Error: asset_id is required"
        nodes = self.facade.get_lineage(asset_id, direction)
        if not nodes:
            return f"No {direction} lineage found for {asset_id}"
        lines = [f"{direction.title()} lineage for {asset_id}:"]
        for n in nodes:
            lines.append(f"  {n['type']}: {n['name']} ({n['id']})")
        return "\n".join(lines)
