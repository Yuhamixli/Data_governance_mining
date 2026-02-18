"""Quality-Embedded Retrieval — inject quality signals directly into data.

Traditional governance: quality scores live in a separate report.
AI-first governance: quality scores are PART OF the data itself.

When an AI agent retrieves knowledge from the vector store, the quality
metadata travels WITH the data. The agent can then:
- Weight high-quality chunks higher in its reasoning
- Skip or discount low-quality chunks
- Understand the freshness and provenance of each piece of knowledge
- Make informed decisions about how much to trust each source

This is the bridge between data governance and data consumption.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.profiler.metrics import QualityMetrics


class QualityEmbedder:
    """Embeds quality metadata directly into ChromaDB chunk metadata.

    After running this, every chunk in the vector store carries:
    - quality_score: 0-1 overall quality
    - freshness_score: 0-1 how fresh the source is
    - content_hash: for dedup detection
    - governance_ts: when quality was last assessed
    - is_quarantined: whether the chunk should be deprioritized

    This enables quality-aware retrieval by the consuming agent.
    """

    QUALITY_FIELDS = [
        "quality_score",
        "freshness_score",
        "content_hash",
        "governance_ts",
        "is_quarantined",
        "char_count",
        "gibberish_score",
    ]

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()
        self.metrics = QualityMetrics()

    def embed_quality_scores(
        self,
        collection_path: str,
        collection_name: str = "xnobot_kb",
        batch_size: int = 100,
    ) -> dict[str, Any]:
        """Compute and embed quality scores into all chunks of a collection.

        Args:
            collection_path: Path to ChromaDB persist directory.
            collection_name: Collection name.
            batch_size: Processing batch size.

        Returns:
            Summary of the embedding operation.
        """
        import chromadb

        client = chromadb.PersistentClient(path=collection_path)
        collection = client.get_collection(collection_name)
        data = collection.get(include=["documents", "metadatas"])

        documents = data.get("documents", [])
        metadatas = data.get("metadatas", [])
        ids = data.get("ids", [])

        now = datetime.now().isoformat()
        updated = 0
        quarantined = 0

        for i in range(0, len(documents), batch_size):
            batch_ids = ids[i:i + batch_size]
            batch_docs = documents[i:i + batch_size]
            batch_metas = metadatas[i:i + batch_size]

            new_metadatas = []
            for doc, meta in zip(batch_docs, batch_metas):
                enriched = dict(meta) if meta else {}

                if not doc or not doc.strip():
                    enriched["quality_score"] = 0.0
                    enriched["is_quarantined"] = True
                    quarantined += 1
                else:
                    # Compute quality signals
                    gibberish = self.metrics.gibberish_score(doc)
                    completeness = self.metrics.completeness_score(
                        doc, min_length=self.config.profiler.min_chunk_length
                    )
                    char_density = self.metrics.char_density(doc)

                    quality = (
                        (1.0 - gibberish) * 0.4
                        + completeness * 0.3
                        + min(char_density / 0.7, 1.0) * 0.3
                    )

                    enriched["quality_score"] = round(quality, 3)
                    enriched["gibberish_score"] = round(gibberish, 3)
                    enriched["char_count"] = len(doc)
                    enriched["content_hash"] = self.metrics.content_hash(doc)
                    enriched["is_quarantined"] = quality < 0.3

                    if quality < 0.3:
                        quarantined += 1

                # Source freshness (if source path available)
                source_path = enriched.get("source", "")
                if source_path:
                    try:
                        import os
                        mtime = os.path.getmtime(source_path)
                        enriched["freshness_score"] = round(
                            self.metrics.freshness_score(
                                mtime,
                                datetime.now().timestamp(),
                                self.config.freshness.stale_threshold_days,
                            ),
                            3,
                        )
                    except OSError:
                        enriched["freshness_score"] = 0.5

                enriched["governance_ts"] = now
                new_metadatas.append(enriched)
                updated += 1

            # Update metadatas in ChromaDB
            collection.update(ids=batch_ids, metadatas=new_metadatas)

        return {
            "total_chunks": len(documents),
            "updated": updated,
            "quarantined": quarantined,
            "timestamp": now,
        }

    def get_quality_summary(
        self,
        collection_path: str,
        collection_name: str = "xnobot_kb",
    ) -> dict[str, Any]:
        """Get aggregate quality statistics from embedded metadata.

        Returns a summary that an AI agent can use to understand
        the overall quality of knowledge it's retrieving from.
        """
        import chromadb

        client = chromadb.PersistentClient(path=collection_path)
        collection = client.get_collection(collection_name)
        data = collection.get(include=["metadatas"])
        metadatas = data.get("metadatas", [])

        if not metadatas:
            return {"total": 0, "has_quality_scores": False}

        scores = [m.get("quality_score") for m in metadatas if m.get("quality_score") is not None]
        quarantined = sum(1 for m in metadatas if m.get("is_quarantined"))
        has_governance = sum(1 for m in metadatas if m.get("governance_ts"))

        if not scores:
            return {
                "total": len(metadatas),
                "has_quality_scores": False,
                "governance_coverage": 0.0,
            }

        return {
            "total": len(metadatas),
            "has_quality_scores": True,
            "governance_coverage": has_governance / len(metadatas),
            "avg_quality": sum(scores) / len(scores),
            "min_quality": min(scores),
            "max_quality": max(scores),
            "quarantined": quarantined,
            "quarantine_rate": quarantined / len(metadatas),
            "quality_distribution": {
                "excellent": sum(1 for s in scores if s >= 0.8) / len(scores),
                "good": sum(1 for s in scores if 0.6 <= s < 0.8) / len(scores),
                "fair": sum(1 for s in scores if 0.3 <= s < 0.6) / len(scores),
                "poor": sum(1 for s in scores if s < 0.3) / len(scores),
            },
        }

    @staticmethod
    def quality_aware_filter(
        results: dict[str, Any],
        min_quality: float = 0.3,
        exclude_quarantined: bool = True,
    ) -> dict[str, Any]:
        """Post-filter ChromaDB query results by quality.

        Use this in the retrieval pipeline to filter out low-quality chunks
        BEFORE they reach the LLM context.

        Args:
            results: Raw ChromaDB query results.
            min_quality: Minimum quality score to include.
            exclude_quarantined: Whether to exclude quarantined chunks.

        Returns:
            Filtered results in the same format.
        """
        if not results or "metadatas" not in results:
            return results

        filtered_indices = []
        metadatas_list = results["metadatas"]

        # Handle both single-query and multi-query result formats
        if metadatas_list and isinstance(metadatas_list[0], list):
            # Multi-query format: [[meta1, meta2], [meta3, meta4]]
            new_results = {k: [] for k in results}
            for q_idx in range(len(metadatas_list)):
                q_filtered = []
                for i, meta in enumerate(metadatas_list[q_idx]):
                    quality = meta.get("quality_score", 1.0)  # Default high if no score
                    quarantined = meta.get("is_quarantined", False)

                    if quality >= min_quality and not (exclude_quarantined and quarantined):
                        q_filtered.append(i)

                for key in results:
                    if isinstance(results[key], list) and len(results[key]) > q_idx:
                        if isinstance(results[key][q_idx], list):
                            new_results[key].append(
                                [results[key][q_idx][i] for i in q_filtered]
                            )
                        else:
                            new_results[key].append(results[key][q_idx])
            return new_results
        else:
            # Single list format
            filtered = []
            for i, meta in enumerate(metadatas_list):
                quality = meta.get("quality_score", 1.0)
                quarantined = meta.get("is_quarantined", False)
                if quality >= min_quality and not (exclude_quarantined and quarantined):
                    filtered.append(i)

            new_results = {}
            for key in results:
                if isinstance(results[key], list):
                    new_results[key] = [results[key][i] for i in filtered]
                else:
                    new_results[key] = results[key]
            return new_results
