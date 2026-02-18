"""Chunk-level quality profiling for vector store data."""

from __future__ import annotations

from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.core.models import (
    DataAssetType,
    QualityDimension,
    QualityReport,
    QualityScore,
)
from data_governance.profiler.metrics import QualityMetrics


class ChunkProfiler:
    """Profile quality of text chunks in the vector store.

    Works directly with ChromaDB collections to assess chunk quality,
    detecting issues like empty chunks, duplicates, gibberish, and
    chunks that are too short or too long.
    """

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()
        self.metrics = QualityMetrics()

    def profile_chunks(
        self,
        chunks: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> QualityReport:
        """Profile a list of text chunks.

        Args:
            chunks: List of text chunk contents.
            metadatas: Optional list of metadata dicts per chunk.
            ids: Optional list of chunk IDs.
        """
        if not chunks:
            return QualityReport(
                asset_id="chunks:empty",
                asset_name="Empty Chunk Set",
                asset_type=DataAssetType.CHUNK,
                overall_score=0.0,
                recommendations=["No chunks to profile"],
            )

        total = len(chunks)
        metadatas = metadatas or [{}] * total
        ids = ids or [f"chunk_{i}" for i in range(total)]

        # --- Completeness ---
        empty_count = sum(1 for c in chunks if not c or not c.strip())
        short_count = sum(
            1
            for c in chunks
            if c and len(c.strip()) < self.config.profiler.min_chunk_length
        )
        completeness = 1.0 - (empty_count + short_count * 0.5) / total
        completeness = max(0.0, completeness)

        # --- Uniqueness (hash-based dedup check) ---
        hashes = [self.metrics.content_hash(c) for c in chunks]
        unique_count = len(set(hashes))
        uniqueness = unique_count / total if total > 0 else 1.0

        # Find duplicate groups
        from collections import Counter

        hash_counts = Counter(hashes)
        duplicate_groups = {h: cnt for h, cnt in hash_counts.items() if cnt > 1}
        duplicate_chunk_count = sum(cnt - 1 for cnt in duplicate_groups.values())

        # --- Validity (content quality) ---
        gibberish_scores = [self.metrics.gibberish_score(c) for c in chunks]
        avg_gibberish = sum(gibberish_scores) / total
        validity = 1.0 - avg_gibberish

        # --- Consistency (length distribution) ---
        lengths = [len(c) for c in chunks if c]
        if lengths:
            mean_len = sum(lengths) / len(lengths)
            variance = sum((l - mean_len) ** 2 for l in lengths) / len(lengths)
            std_len = variance**0.5
            cv = std_len / mean_len if mean_len > 0 else 0
            consistency = max(0.0, 1.0 - cv)  # Low CV = high consistency
        else:
            consistency = 0.0

        # --- Source diversity ---
        sources = [m.get("source", "unknown") for m in metadatas]
        unique_sources = set(sources)

        dimension_scores = [
            QualityScore(
                dimension=QualityDimension.COMPLETENESS,
                score=completeness,
                details=f"{empty_count} empty, {short_count} too short out of {total}",
                metrics={
                    "total": total,
                    "empty_count": empty_count,
                    "short_count": short_count,
                },
            ),
            QualityScore(
                dimension=QualityDimension.UNIQUENESS,
                score=uniqueness,
                details=f"{duplicate_chunk_count} duplicates across {len(duplicate_groups)} groups",
                metrics={
                    "unique_count": unique_count,
                    "duplicate_chunk_count": duplicate_chunk_count,
                    "duplicate_groups": len(duplicate_groups),
                },
            ),
            QualityScore(
                dimension=QualityDimension.VALIDITY,
                score=validity,
                details=f"Average gibberish score: {avg_gibberish:.3f}",
                metrics={
                    "avg_gibberish": avg_gibberish,
                    "max_gibberish": max(gibberish_scores) if gibberish_scores else 0,
                },
            ),
            QualityScore(
                dimension=QualityDimension.CONSISTENCY,
                score=consistency,
                details=f"Length stats — mean: {mean_len:.0f}, std: {std_len:.0f}"
                if lengths
                else "No valid chunks",
                metrics={
                    "mean_length": mean_len if lengths else 0,
                    "std_length": std_len if lengths else 0,
                    "min_length": min(lengths) if lengths else 0,
                    "max_length": max(lengths) if lengths else 0,
                    "unique_sources": len(unique_sources),
                },
            ),
        ]

        overall = sum(s.score for s in dimension_scores) / len(dimension_scores)
        issues = sum(1 for s in dimension_scores if s.score < 0.8)

        recommendations = self._generate_recommendations(
            dimension_scores, total, duplicate_chunk_count, empty_count
        )

        return QualityReport(
            asset_id="chunks:collection",
            asset_name=f"Chunk Collection ({total} chunks)",
            asset_type=DataAssetType.CHUNK,
            overall_score=overall,
            dimension_scores=dimension_scores,
            total_items=total,
            issues_found=issues,
            recommendations=recommendations,
        )

    def profile_chromadb_collection(
        self, collection_path: str, collection_name: str = "xnobot_kb"
    ) -> QualityReport:
        """Profile a ChromaDB collection directly.

        Args:
            collection_path: Path to the ChromaDB persist directory.
            collection_name: Name of the collection to profile.
        """
        try:
            import chromadb

            client = chromadb.PersistentClient(path=collection_path)
            collection = client.get_collection(collection_name)
            result = collection.get(include=["documents", "metadatas"])

            chunks = result.get("documents", [])
            metadatas = result.get("metadatas", [])
            ids = result.get("ids", [])

            report = self.profile_chunks(chunks, metadatas, ids)
            report.asset_id = f"chromadb:{collection_name}"
            report.asset_name = f"ChromaDB: {collection_name}"
            report.asset_type = DataAssetType.COLLECTION
            return report

        except Exception as e:
            return QualityReport(
                asset_id=f"chromadb:{collection_name}",
                asset_name=f"ChromaDB: {collection_name}",
                asset_type=DataAssetType.COLLECTION,
                overall_score=0.0,
                recommendations=[f"Failed to connect to ChromaDB: {e}"],
            )

    def find_problematic_chunks(
        self, chunks: list[str], ids: list[str] | None = None
    ) -> dict[str, list[dict[str, Any]]]:
        """Identify specific problematic chunks by category.

        Returns a dict with keys: 'empty', 'too_short', 'too_long', 'gibberish', 'duplicate'.
        """
        ids = ids or [f"chunk_{i}" for i in range(len(chunks))]
        problems: dict[str, list[dict[str, Any]]] = {
            "empty": [],
            "too_short": [],
            "too_long": [],
            "gibberish": [],
            "duplicate": [],
        }

        seen_hashes: dict[str, str] = {}  # hash -> first id

        for i, (chunk, chunk_id) in enumerate(zip(chunks, ids)):
            if not chunk or not chunk.strip():
                problems["empty"].append({"id": chunk_id, "index": i})
                continue

            length = len(chunk.strip())
            if length < self.config.profiler.min_chunk_length:
                problems["too_short"].append(
                    {"id": chunk_id, "index": i, "length": length}
                )

            if length > self.config.profiler.max_chunk_length:
                problems["too_long"].append(
                    {"id": chunk_id, "index": i, "length": length}
                )

            gibberish = self.metrics.gibberish_score(chunk)
            if gibberish > 0.3:
                problems["gibberish"].append(
                    {"id": chunk_id, "index": i, "score": gibberish}
                )

            content_hash = self.metrics.content_hash(chunk)
            if content_hash in seen_hashes:
                problems["duplicate"].append(
                    {
                        "id": chunk_id,
                        "index": i,
                        "duplicate_of": seen_hashes[content_hash],
                    }
                )
            else:
                seen_hashes[content_hash] = chunk_id

        return problems

    def _generate_recommendations(
        self,
        scores: list[QualityScore],
        total: int,
        duplicate_count: int,
        empty_count: int,
    ) -> list[str]:
        recs: list[str] = []
        if empty_count > 0:
            recs.append(f"Remove {empty_count} empty chunks from the collection")
        if duplicate_count > 0:
            recs.append(
                f"Deduplicate {duplicate_count} duplicate chunks to improve search relevance"
            )
        for s in scores:
            if s.dimension == QualityDimension.VALIDITY and s.score < 0.7:
                recs.append("Review chunks with high gibberish scores for encoding issues")
            if s.dimension == QualityDimension.CONSISTENCY and s.score < 0.6:
                recs.append("Consider normalizing chunk sizes for more consistent retrieval")
        return recs
