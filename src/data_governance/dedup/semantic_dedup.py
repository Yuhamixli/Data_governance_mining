"""Semantic deduplication using embedding similarity."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from data_governance.core.config import GovernanceConfig
from data_governance.dedup.hash_dedup import DedupResult, DuplicateGroup


class SemanticDeduplicator:
    """Near-duplicate detection using embedding cosine similarity.

    Finds chunks that are semantically equivalent even if not character-identical
    (e.g., different whitespace, minor edits, reformatting).
    """

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()
        self.threshold = config.dedup.semantic_threshold if config else 0.95

    def find_near_duplicates(
        self,
        embeddings: list[list[float]] | np.ndarray,
        ids: list[str] | None = None,
        threshold: float | None = None,
    ) -> DedupResult:
        """Find near-duplicates by cosine similarity of embeddings.

        Args:
            embeddings: List of embedding vectors.
            ids: Optional IDs for each item.
            threshold: Similarity threshold (default from config).
        """
        threshold = threshold or self.threshold
        if isinstance(embeddings, list):
            embeddings = np.array(embeddings)

        n = len(embeddings)
        ids = ids or [f"item_{i}" for i in range(n)]

        # Normalize for cosine similarity
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        normalized = embeddings / norms

        # Union-Find for grouping
        parent = list(range(n))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x: int, y: int) -> None:
            rx, ry = find(x), find(y)
            if rx != ry:
                parent[rx] = ry

        # Find pairs above threshold (batch cosine similarity)
        batch_size = self.config.dedup.batch_size if self.config else 100
        for i in range(0, n, batch_size):
            batch_end = min(i + batch_size, n)
            batch = normalized[i:batch_end]
            # Compute similarity of batch against all items
            sim_matrix = batch @ normalized.T
            for bi in range(batch.shape[0]):
                global_i = i + bi
                for j in range(global_i + 1, n):
                    if sim_matrix[bi, j] >= threshold:
                        union(global_i, j)

        # Collect groups
        groups: dict[int, list[int]] = {}
        for i in range(n):
            root = find(i)
            if root not in groups:
                groups[root] = []
            groups[root].append(i)

        result = DedupResult(total_items=n)
        unique_count = len(groups)

        for root, members in groups.items():
            if len(members) > 1:
                dup_group = DuplicateGroup(
                    hash_value=f"semantic_group_{root}",
                    ids=[ids[m] for m in members],
                    keep_id=ids[members[0]],
                    remove_ids=[ids[m] for m in members[1:]],
                )
                result.duplicate_groups.append(dup_group)
                result.removed_ids.extend(dup_group.remove_ids)

        result.unique_items = unique_count
        return result

    def find_near_duplicates_in_chromadb(
        self,
        collection_path: str,
        collection_name: str = "nanobot_kb",
        threshold: float | None = None,
    ) -> DedupResult:
        """Find near-duplicates in a ChromaDB collection using stored embeddings.

        Args:
            collection_path: Path to ChromaDB persist directory.
            collection_name: Collection name.
            threshold: Similarity threshold.
        """
        import chromadb

        client = chromadb.PersistentClient(path=collection_path)
        collection = client.get_collection(collection_name)
        data = collection.get(include=["embeddings"])

        embeddings = data.get("embeddings", [])
        ids = data.get("ids", [])

        if not embeddings:
            return DedupResult(total_items=0, unique_items=0)

        return self.find_near_duplicates(embeddings, ids, threshold)
