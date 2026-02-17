"""GovernanceFacade — unified high-level API for all governance operations.

This is the primary integration point for nanobot and other consumers.
All operations are exposed through a single facade class with simple methods.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.core.models import QualityReport, ValidationResult
from data_governance.dedup.engine import DedupEngine, FullDedupReport
from data_governance.dedup.hash_dedup import DedupResult
from data_governance.freshness.tracker import FreshnessReport, FreshnessTracker
from data_governance.lineage.tracker import LineageTracker
from data_governance.profiler.chunk import ChunkProfiler
from data_governance.profiler.collection import CollectionProfiler
from data_governance.profiler.document import DocumentProfiler
from data_governance.reporter.alerts import AlertManager
from data_governance.reporter.health import HealthReporter, HealthScore
from data_governance.validation.engine import ValidationEngine


class GovernanceFacade:
    """Unified facade for all data governance operations.

    Usage:
        from data_governance.api import GovernanceFacade

        gov = GovernanceFacade(
            workspace_path="/path/to/nanobot/workspace",
            chromadb_path="/path/to/workspace/knowledge_db",
        )

        # Full health check
        health = gov.health_check()
        print(health.summary)

        # Dedup
        dedup = gov.find_duplicates()
        print(dedup.summary())

        # Validate
        results = gov.validate_knowledge_base()
    """

    def __init__(
        self,
        workspace_path: str | Path | None = None,
        chromadb_path: str | Path | None = None,
        collection_name: str = "nanobot_kb",
        config: GovernanceConfig | None = None,
    ):
        self.workspace_path = Path(workspace_path) if workspace_path else Path(".")
        self.chromadb_path = str(chromadb_path) if chromadb_path else None
        self.collection_name = collection_name

        if config:
            self.config = config
        else:
            self.config = GovernanceConfig(workspace_path=str(self.workspace_path))

        governance_db = self.workspace_path / ".governance"

        # Initialize components
        self.doc_profiler = DocumentProfiler(self.config)
        self.chunk_profiler = ChunkProfiler(self.config)
        self.collection_profiler = CollectionProfiler(self.config)
        self.dedup_engine = DedupEngine(self.config)
        self.validation_engine = ValidationEngine(self.config)
        self.freshness_tracker = FreshnessTracker(self.config)
        self.lineage_tracker = LineageTracker(governance_db / "lineage.json")
        self.health_reporter = HealthReporter(self.config)
        self.alert_manager = AlertManager(governance_db / "alerts.json")

    # ── Health Check ──────────────────────────────────────────────

    def health_check(self) -> HealthScore:
        """Run a comprehensive health check on the knowledge base.

        Returns an overall health score with component breakdown.
        """
        quality_report = None
        validation_results = None
        dedup_result = None
        freshness_report = None

        knowledge_dir = self._knowledge_dir()

        # Quality profiling
        if knowledge_dir and knowledge_dir.exists():
            quality_report = self.collection_profiler.profile_knowledge_base(
                knowledge_dir=knowledge_dir,
                chromadb_path=self.chromadb_path,
                collection_name=self.collection_name,
            )

        # Validation
        if self.chromadb_path:
            validation_results = self.validation_engine.validate_chromadb_collection(
                self.chromadb_path, self.collection_name
            )

        # Dedup check
        if self.chromadb_path:
            hash_result = self.dedup_engine.hash_dedup.deduplicate_chromadb(
                self.chromadb_path, self.collection_name, dry_run=True
            )
            dedup_result = hash_result

        # Freshness
        if knowledge_dir and knowledge_dir.exists():
            freshness_report = self.freshness_tracker.scan_knowledge_dir(knowledge_dir)

        # Compute health
        health = self.health_reporter.compute_health(
            quality_report=quality_report,
            validation_results=validation_results,
            dedup_result=dedup_result,
            freshness_report=freshness_report,
        )

        # Check for alerts
        self.alert_manager.check_health_alerts(
            health.overall,
            threshold=self.config.reporter.alert_threshold,
            component_scores={c.name: c.score for c in health.components},
        )

        # Save history
        governance_db = self.workspace_path / ".governance"
        self.health_reporter.save_history(health, governance_db / "health_history.jsonl")

        return health

    # ── Profiling ─────────────────────────────────────────────────

    def profile_document(self, file_path: str | Path) -> QualityReport:
        """Profile a single document's quality."""
        return self.doc_profiler.profile_file(file_path)

    def profile_knowledge_base(self) -> QualityReport:
        """Profile the entire knowledge base."""
        knowledge_dir = self._knowledge_dir()
        if not knowledge_dir or not knowledge_dir.exists():
            return QualityReport(
                asset_id="kb:full",
                asset_name="Knowledge Base",
                asset_type="collection",
                overall_score=0.0,
                recommendations=["Knowledge directory not found"],
            )
        return self.collection_profiler.profile_knowledge_base(
            knowledge_dir=knowledge_dir,
            chromadb_path=self.chromadb_path,
            collection_name=self.collection_name,
        )

    def profile_chunks(self) -> QualityReport:
        """Profile the vector store chunks."""
        if not self.chromadb_path:
            return QualityReport(
                asset_id="chunks:none",
                asset_name="No ChromaDB configured",
                asset_type="chunk",
                overall_score=0.0,
            )
        return self.chunk_profiler.profile_chromadb_collection(
            self.chromadb_path, self.collection_name
        )

    # ── Deduplication ─────────────────────────────────────────────

    def find_duplicates(self, include_semantic: bool = False) -> FullDedupReport:
        """Find duplicates in the vector store.

        Args:
            include_semantic: Also run semantic near-duplicate detection.
        """
        if not self.chromadb_path:
            return FullDedupReport(
                hash_result=DedupResult(),
                actions_taken=["No ChromaDB path configured"],
            )
        return self.dedup_engine.dedup_chromadb_collection(
            self.chromadb_path,
            self.collection_name,
            include_semantic=include_semantic,
            dry_run=True,
        )

    def remove_duplicates(self, include_semantic: bool = False) -> FullDedupReport:
        """Remove duplicates from the vector store (destructive).

        Args:
            include_semantic: Also remove semantic near-duplicates.
        """
        if not self.chromadb_path:
            return FullDedupReport(
                hash_result=DedupResult(),
                actions_taken=["No ChromaDB path configured"],
            )
        return self.dedup_engine.dedup_chromadb_collection(
            self.chromadb_path,
            self.collection_name,
            include_semantic=include_semantic,
            dry_run=False,
        )

    # ── Validation ────────────────────────────────────────────────

    def validate_knowledge_base(self) -> list[ValidationResult]:
        """Validate knowledge base chunks."""
        if not self.chromadb_path:
            return []
        return self.validation_engine.validate_chromadb_collection(
            self.chromadb_path, self.collection_name
        )

    def validate_documents(self) -> list[ValidationResult]:
        """Validate document files on disk."""
        knowledge_dir = self._knowledge_dir()
        if not knowledge_dir or not knowledge_dir.exists():
            return []
        return self.validation_engine.validate_document_files(knowledge_dir)

    def validate_chat_history(self) -> list[ValidationResult]:
        """Validate chat history files."""
        history_dir = self.workspace_path / "chat_history"
        if not history_dir.exists():
            return []
        return self.validation_engine.validate_chat_history(history_dir)

    def validate_memory(self) -> list[ValidationResult]:
        """Validate memory files."""
        memory_dir = self.workspace_path / "memory"
        if not memory_dir.exists():
            return []
        return self.validation_engine.validate_memory_files(memory_dir)

    # ── Freshness ─────────────────────────────────────────────────

    def check_freshness(self) -> FreshnessReport:
        """Check freshness of all knowledge base assets."""
        knowledge_dir = self._knowledge_dir()
        if not knowledge_dir or not knowledge_dir.exists():
            return FreshnessReport()
        return self.freshness_tracker.scan_knowledge_dir(knowledge_dir)

    def get_stale_assets(self) -> list[dict[str, Any]]:
        """Get list of stale assets needing attention."""
        report = self.check_freshness()
        return self.freshness_tracker.get_stale_assets(report)

    def get_expired_assets(self) -> list[dict[str, Any]]:
        """Get list of expired assets with recommended actions."""
        report = self.check_freshness()
        return self.freshness_tracker.get_expired_assets(report)

    # ── Lineage ───────────────────────────────────────────────────

    def get_lineage(self, asset_id: str, direction: str = "upstream") -> list[dict[str, Any]]:
        """Get lineage for an asset.

        Args:
            asset_id: Asset identifier (e.g., "file:doc.md", "chunk:abc123").
            direction: "upstream" (sources) or "downstream" (consumers).
        """
        if direction == "upstream":
            nodes = self.lineage_tracker.get_source_lineage(asset_id)
        else:
            nodes = self.lineage_tracker.get_impact_analysis(asset_id)
        return [{"id": n.id, "name": n.name, "type": n.node_type} for n in nodes]

    def get_lineage_diagram(self) -> str:
        """Get lineage graph as Mermaid diagram."""
        return self.lineage_tracker.export_mermaid()

    # ── Alerts ────────────────────────────────────────────────────

    def get_alerts(self) -> list[dict[str, Any]]:
        """Get all active (unresolved) alerts."""
        return [a.to_dict() for a in self.alert_manager.get_active_alerts()]

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        return self.alert_manager.acknowledge(alert_id)

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        return self.alert_manager.resolve(alert_id)

    # ── Helpers ───────────────────────────────────────────────────

    def _knowledge_dir(self) -> Path | None:
        """Get the knowledge directory path."""
        knowledge_dir = self.workspace_path / "knowledge"
        if knowledge_dir.exists():
            return knowledge_dir
        return None
