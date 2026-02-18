"""Collection-level quality profiling — aggregates document and chunk profiling."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.core.models import (
    DataAssetType,
    QualityDimension,
    QualityReport,
    QualityScore,
)
from data_governance.profiler.chunk import ChunkProfiler
from data_governance.profiler.document import DocumentProfiler
from data_governance.profiler.metrics import QualityMetrics


class CollectionProfiler:
    """Aggregate profiler for an entire knowledge base (documents + vectors).

    Combines document-level file profiling with chunk-level vector store
    profiling to produce a unified health assessment.
    """

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()
        self.doc_profiler = DocumentProfiler(config)
        self.chunk_profiler = ChunkProfiler(config)
        self.metrics = QualityMetrics()

    def profile_knowledge_base(
        self,
        knowledge_dir: str | Path,
        chromadb_path: str | Path | None = None,
        collection_name: str = "xnobot_kb",
    ) -> QualityReport:
        """Profile the full knowledge base: files + vector store.

        Args:
            knowledge_dir: Path to the knowledge directory (containing documents).
            chromadb_path: Path to ChromaDB persist directory.
            collection_name: ChromaDB collection name.
        """
        knowledge_dir = Path(knowledge_dir)
        dimension_scores: list[QualityScore] = []
        total_items = 0
        total_issues = 0
        recommendations: list[str] = []

        # --- Profile documents ---
        doc_reports = self.doc_profiler.profile_directory(knowledge_dir)
        total_items += len(doc_reports)
        doc_issues = sum(r.issues_found for r in doc_reports)
        total_issues += doc_issues

        if doc_reports:
            avg_doc_score = sum(r.overall_score for r in doc_reports) / len(doc_reports)
            low_quality_docs = [r for r in doc_reports if r.overall_score < 0.6]
            if low_quality_docs:
                recommendations.append(
                    f"{len(low_quality_docs)} documents have low quality scores — review them"
                )
            for r in doc_reports:
                recommendations.extend(r.recommendations)
        else:
            avg_doc_score = 0.0
            recommendations.append("No documents found in knowledge directory")

        dimension_scores.append(
            QualityScore(
                dimension=QualityDimension.COMPLETENESS,
                score=avg_doc_score,
                details=f"{len(doc_reports)} documents profiled, avg score: {avg_doc_score:.1%}",
                metrics={
                    "document_count": len(doc_reports),
                    "avg_document_score": avg_doc_score,
                    "low_quality_count": len(low_quality_docs) if doc_reports else 0,
                },
            )
        )

        # --- Profile vector store ---
        if chromadb_path:
            chunk_report = self.chunk_profiler.profile_chromadb_collection(
                str(chromadb_path), collection_name
            )
            total_items += chunk_report.total_items
            total_issues += chunk_report.issues_found
            recommendations.extend(chunk_report.recommendations)

            chunk_uniqueness = chunk_report.get_dimension_score(QualityDimension.UNIQUENESS) or 0.0
            chunk_validity = chunk_report.get_dimension_score(QualityDimension.VALIDITY) or 0.0

            dimension_scores.append(
                QualityScore(
                    dimension=QualityDimension.UNIQUENESS,
                    score=chunk_uniqueness,
                    details=f"Vector store: {chunk_report.total_items} chunks",
                    metrics={"chunk_count": chunk_report.total_items},
                )
            )
            dimension_scores.append(
                QualityScore(
                    dimension=QualityDimension.VALIDITY,
                    score=chunk_validity,
                    details="Chunk content validity assessment",
                )
            )

        # --- File-Vector consistency check ---
        if chromadb_path and doc_reports:
            consistency = self._check_file_vector_consistency(
                knowledge_dir, str(chromadb_path), collection_name
            )
            dimension_scores.append(consistency)
            if consistency.score < 0.8:
                total_issues += 1
                recommendations.append(
                    "File-vector store inconsistency detected — re-ingest documents"
                )

        # --- Freshness assessment ---
        if doc_reports:
            timeliness_scores = []
            for r in doc_reports:
                ts = r.get_dimension_score(QualityDimension.TIMELINESS)
                if ts is not None:
                    timeliness_scores.append(ts)
            avg_freshness = (
                sum(timeliness_scores) / len(timeliness_scores) if timeliness_scores else 0.5
            )
            dimension_scores.append(
                QualityScore(
                    dimension=QualityDimension.TIMELINESS,
                    score=avg_freshness,
                    details=f"Average document freshness: {avg_freshness:.1%}",
                )
            )

        overall = sum(s.score for s in dimension_scores) / len(dimension_scores) if dimension_scores else 0.0

        # Deduplicate recommendations
        recommendations = list(dict.fromkeys(recommendations))

        return QualityReport(
            asset_id="kb:full",
            asset_name="Knowledge Base (Full Assessment)",
            asset_type=DataAssetType.COLLECTION,
            overall_score=overall,
            dimension_scores=dimension_scores,
            total_items=total_items,
            issues_found=total_issues,
            recommendations=recommendations,
        )

    def _check_file_vector_consistency(
        self, knowledge_dir: Path, chromadb_path: str, collection_name: str
    ) -> QualityScore:
        """Check if documents on disk match what's in the vector store."""
        try:
            import chromadb

            client = chromadb.PersistentClient(path=chromadb_path)
            collection = client.get_collection(collection_name)
            result = collection.get(include=["metadatas"])
            metadatas = result.get("metadatas", [])

            vector_sources = set()
            for m in metadatas:
                source = m.get("source", "")
                if source:
                    vector_sources.add(Path(source).name)

            file_names = set()
            for ext in DocumentProfiler.SUPPORTED_EXTENSIONS:
                for f in knowledge_dir.rglob(f"*{ext}"):
                    file_names.add(f.name)

            if not file_names and not vector_sources:
                return QualityScore(
                    dimension=QualityDimension.CONSISTENCY,
                    score=1.0,
                    details="Both empty — consistent",
                )

            all_names = file_names | vector_sources
            overlap = file_names & vector_sources
            consistency = len(overlap) / len(all_names) if all_names else 1.0

            orphaned_files = file_names - vector_sources
            orphaned_vectors = vector_sources - file_names

            return QualityScore(
                dimension=QualityDimension.CONSISTENCY,
                score=consistency,
                details=(
                    f"Files: {len(file_names)}, Vectors: {len(vector_sources)}, "
                    f"Overlap: {len(overlap)}"
                ),
                metrics={
                    "file_count": len(file_names),
                    "vector_source_count": len(vector_sources),
                    "overlap_count": len(overlap),
                    "orphaned_files": list(orphaned_files)[:20],
                    "orphaned_vectors": list(orphaned_vectors)[:20],
                },
            )
        except Exception as e:
            return QualityScore(
                dimension=QualityDimension.CONSISTENCY,
                score=0.5,
                details=f"Could not verify consistency: {e}",
            )
