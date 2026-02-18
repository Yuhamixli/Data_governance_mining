"""GovernanceDaemon — continuous background data quality guardian.

Not a human-triggered cron job. An always-on daemon that:
1. Watches for data changes (new files, modified chunks)
2. Continuously assesses quality in the background
3. Auto-remediates within safe bounds
4. Emits structured signals for consuming agents

Designed to be embedded in nanobot's heartbeat service or run standalone.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from data_governance.agent.governance_agent import GovernanceAgent
from data_governance.core.config import GovernanceConfig
from data_governance.protocol.quality_embed import QualityEmbedder

logger = logging.getLogger("data_governance.daemon")


class GovernanceDaemon:
    """Continuous data quality monitoring daemon.

    Usage as standalone:
        daemon = GovernanceDaemon(
            workspace_path="~/.nanobot/workspace",
            chromadb_path="~/.nanobot/workspace/knowledge_db",
        )
        asyncio.run(daemon.run())

    Usage embedded in nanobot heartbeat:
        daemon = GovernanceDaemon(...)

        # In heartbeat tick:
        result = daemon.tick()
        if result["actions_taken"]:
            logger.info(f"Governance: {result['summary']}")

    Usage for event-driven governance:
        daemon = GovernanceDaemon(...)

        # After document ingestion:
        daemon.on_ingest(file_path="doc.md", chunk_ids=["c1", "c2"])

        # After web cache update:
        daemon.on_web_cache(url="...", cache_path="...")
    """

    def __init__(
        self,
        workspace_path: str | Path | None = None,
        chromadb_path: str | Path | None = None,
        collection_name: str = "nanobot_kb",
        config: GovernanceConfig | None = None,
        check_interval_seconds: int = 3600,
        on_alert: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.agent = GovernanceAgent(
            workspace_path=workspace_path,
            chromadb_path=chromadb_path,
            collection_name=collection_name,
            config=config,
        )
        self.quality_embedder = QualityEmbedder(config)
        self.chromadb_path = str(chromadb_path) if chromadb_path else None
        self.collection_name = collection_name
        self.check_interval = check_interval_seconds
        self.on_alert = on_alert
        self._running = False
        self._last_tick_result: dict[str, Any] | None = None

    # ── Continuous Run Mode ───────────────────────────────────────

    async def run(self) -> None:
        """Run the daemon continuously (async).

        This is the standalone mode — the daemon runs its own event loop
        and continuously monitors data quality.
        """
        self._running = True
        logger.info("Governance daemon started")

        while self._running:
            try:
                result = self.tick()
                self._last_tick_result = result

                if result.get("actions_taken"):
                    logger.info(f"Governance tick: {result['summary']}")
                    if self.on_alert and result.get("alerts"):
                        for alert in result["alerts"]:
                            self.on_alert(alert)
                else:
                    logger.debug(f"Governance tick: healthy ({result.get('health_score', '?')})")

            except Exception as e:
                logger.error(f"Governance daemon error: {e}")

            await asyncio.sleep(self.check_interval)

    def stop(self) -> None:
        """Stop the daemon."""
        self._running = False
        logger.info("Governance daemon stopped")

    # ── Single Tick Mode ──────────────────────────────────────────

    def tick(self) -> dict[str, Any]:
        """Run a single governance cycle.

        Returns structured results for the consuming agent/system.
        Call this from nanobot's heartbeat or any periodic task.
        """
        result: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "actions_taken": [],
            "health_score": 0,
            "alerts": [],
            "summary": "",
        }

        # Check if it's time to run
        if not self.agent.should_run():
            result["summary"] = "Skipped — not enough time since last run"
            return result

        # Run full agent cycle
        plan = self.agent.perceive_and_decide()
        result["health_score"] = self.agent.memory.get("last_health_score", 0)

        # Execute safe actions automatically
        if plan.auto_executable_decisions():
            exec_result = self.agent.execute_plan(plan, auto_only=True)
            result["actions_taken"] = exec_result.get("actions", [])

        # Embed quality scores if chromadb available
        if self.chromadb_path:
            try:
                embed_result = self.quality_embedder.embed_quality_scores(
                    self.chromadb_path, self.collection_name
                )
                result["quality_embedded"] = embed_result
            except Exception as e:
                logger.warning(f"Quality embedding failed: {e}")

        # Check for alerts
        active_alerts = self.agent.facade.get_alerts()
        result["alerts"] = active_alerts

        # Generate summary
        plan_output = plan.to_structured_output()
        result["plan"] = plan_output
        result["summary"] = (
            f"Health: {result['health_score']:.0%} | "
            f"Decisions: {plan_output['total_decisions']} | "
            f"Executed: {len(result['actions_taken'])} | "
            f"Alerts: {len(active_alerts)}"
        )

        return result

    # ── Event-Driven Hooks ────────────────────────────────────────

    def on_ingest(
        self, file_path: str, chunk_ids: list[str] | None = None
    ) -> dict[str, Any]:
        """Called after a document is ingested into the knowledge base.

        Performs immediate governance checks on the newly ingested data:
        - Quality profiling of the document
        - Dedup check against existing chunks
        - Quality score embedding for new chunks
        - Lineage recording

        Args:
            file_path: Path to the ingested document.
            chunk_ids: IDs of the created chunks (if known).
        """
        result: dict[str, Any] = {"event": "post_ingest", "file": file_path}

        # Profile the document
        try:
            report = self.agent.facade.profile_document(file_path)
            result["quality_score"] = report.overall_score
            result["quality_issues"] = report.issues_found
        except Exception as e:
            result["profile_error"] = str(e)

        # Record lineage
        if chunk_ids:
            try:
                self.agent.facade.lineage_tracker.record_ingestion(
                    source_path=file_path,
                    chunk_ids=chunk_ids,
                    collection_name=self.collection_name,
                )
                result["lineage_recorded"] = True
            except Exception:
                result["lineage_recorded"] = False

        # Dedup check
        try:
            dedup = self.agent.facade.find_duplicates()
            if dedup.hash_result.duplicate_count > 0:
                result["duplicates_found"] = dedup.hash_result.duplicate_count
                # Auto-remove duplicates
                self.agent.facade.remove_duplicates()
                result["duplicates_removed"] = True
        except Exception:
            pass

        # Re-embed quality scores
        if self.chromadb_path:
            try:
                self.quality_embedder.embed_quality_scores(
                    self.chromadb_path, self.collection_name
                )
            except Exception:
                pass

        return result

    def on_web_cache(self, url: str, cache_path: str) -> dict[str, Any]:
        """Called after web content is cached.

        Records lineage and assesses quality of cached web data.
        """
        result: dict[str, Any] = {"event": "web_cache", "url": url}

        try:
            self.agent.facade.lineage_tracker.record_web_cache(
                url=url,
                cache_path=cache_path,
                collection_name=f"{self.collection_name}_web_cache",
            )
            result["lineage_recorded"] = True
        except Exception:
            result["lineage_recorded"] = False

        return result

    def on_search(
        self,
        query: str,
        results: dict[str, Any],
        min_quality: float = 0.3,
    ) -> dict[str, Any]:
        """Called before search results are passed to the LLM.

        Filters results by quality, ensuring the agent only sees
        trusted data. This is the quality gate.

        Args:
            query: The search query.
            results: Raw ChromaDB query results.
            min_quality: Minimum quality threshold.
        """
        filtered = QualityEmbedder.quality_aware_filter(
            results,
            min_quality=min_quality,
            exclude_quarantined=True,
        )

        original_count = len(results.get("ids", [[]])[0]) if results.get("ids") else 0
        filtered_count = len(filtered.get("ids", [[]])[0]) if filtered.get("ids") else 0
        removed = original_count - filtered_count

        return {
            "event": "search_filter",
            "query": query,
            "original_results": original_count,
            "filtered_results": filtered_count,
            "removed_low_quality": removed,
            "results": filtered,
        }

    # ── State Inspection ──────────────────────────────────────────

    def get_status(self) -> dict[str, Any]:
        """Get daemon status for monitoring."""
        return {
            "running": self._running,
            "last_tick": self._last_tick_result,
            "agent_state": self.agent.get_governance_state(),
        }
