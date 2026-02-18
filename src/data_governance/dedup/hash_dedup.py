"""Hash-based deduplication for exact and near-exact duplicates."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.profiler.metrics import QualityMetrics


@dataclass
class DedupResult:
    """Result of a deduplication operation."""

    total_items: int = 0
    unique_items: int = 0
    duplicate_groups: list[DuplicateGroup] = field(default_factory=list)
    removed_ids: list[str] = field(default_factory=list)

    @property
    def duplicate_count(self) -> int:
        return self.total_items - self.unique_items

    @property
    def duplicate_ratio(self) -> float:
        return self.duplicate_count / self.total_items if self.total_items > 0 else 0.0

    def summary(self) -> str:
        return (
            f"Dedup: {self.total_items} total, {self.unique_items} unique, "
            f"{self.duplicate_count} duplicates ({self.duplicate_ratio:.1%})"
        )


@dataclass
class DuplicateGroup:
    """A group of duplicate items sharing the same hash."""

    hash_value: str
    ids: list[str] = field(default_factory=list)
    keep_id: str = ""
    remove_ids: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.ids)


class HashDeduplicator:
    """Exact content hash-based deduplication.

    Identifies and optionally removes chunks with identical content
    using fast xxhash or SHA-256 content hashing.
    """

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()
        self.metrics = QualityMetrics()

    def find_duplicates(
        self,
        contents: list[str],
        ids: list[str] | None = None,
        metadatas: list[dict[str, Any]] | None = None,
        normalize: bool = True,
    ) -> DedupResult:
        """Find exact duplicates by content hash.

        Args:
            contents: List of text contents to check.
            ids: Optional IDs for each item.
            metadatas: Optional metadata for keep-strategy decisions.
            normalize: Normalize whitespace before hashing.
        """
        ids = ids or [f"item_{i}" for i in range(len(contents))]
        metadatas = metadatas or [{}] * len(contents)

        hash_groups: dict[str, list[tuple[str, int, dict[str, Any]]]] = {}

        for i, (content, item_id, meta) in enumerate(zip(contents, ids, metadatas)):
            text = self._normalize(content) if normalize else content
            h = self.metrics.content_hash(text)
            if h not in hash_groups:
                hash_groups[h] = []
            hash_groups[h].append((item_id, i, meta))

        result = DedupResult(total_items=len(contents))
        unique_count = 0

        for h, group in hash_groups.items():
            unique_count += 1
            if len(group) > 1:
                dup_group = DuplicateGroup(hash_value=h)
                dup_group.ids = [item_id for item_id, _, _ in group]
                # Keep strategy: keep the first one (or the one with most metadata)
                keep_idx = self._select_keep(group)
                dup_group.keep_id = group[keep_idx][0]
                dup_group.remove_ids = [
                    item_id for j, (item_id, _, _) in enumerate(group) if j != keep_idx
                ]
                result.duplicate_groups.append(dup_group)
                result.removed_ids.extend(dup_group.remove_ids)

        result.unique_items = unique_count
        return result

    def deduplicate_chromadb(
        self,
        collection_path: str,
        collection_name: str = "xnobot_kb",
        dry_run: bool = True,
    ) -> DedupResult:
        """Find and optionally remove duplicates from a ChromaDB collection.

        Args:
            collection_path: Path to ChromaDB persist directory.
            collection_name: Collection name.
            dry_run: If True, only report — don't delete.
        """
        import chromadb

        client = chromadb.PersistentClient(path=collection_path)
        collection = client.get_collection(collection_name)
        data = collection.get(include=["documents", "metadatas"])

        contents = data.get("documents", [])
        ids = data.get("ids", [])
        metadatas = data.get("metadatas", [])

        result = self.find_duplicates(contents, ids, metadatas)

        if not dry_run and result.removed_ids:
            # Delete duplicates in batches
            batch_size = self.config.dedup.batch_size
            for i in range(0, len(result.removed_ids), batch_size):
                batch = result.removed_ids[i : i + batch_size]
                collection.delete(ids=batch)

        return result

    def _normalize(self, text: str) -> str:
        """Normalize text for comparison: strip, collapse whitespace."""
        text = text.strip()
        text = re.sub(r"\s+", " ", text)
        return text

    def _select_keep(
        self, group: list[tuple[str, int, dict[str, Any]]]
    ) -> int:
        """Select which item to keep from a duplicate group.

        Strategy: keep the one with the most metadata, or the first one.
        """
        best_idx = 0
        best_meta_count = 0
        for i, (_, _, meta) in enumerate(group):
            meta_count = len(meta)
            if meta_count > best_meta_count:
                best_meta_count = meta_count
                best_idx = i
        return best_idx
