"""Unified deduplication engine combining hash and semantic approaches."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.dedup.hash_dedup import DedupResult, DuplicateGroup, HashDeduplicator
from data_governance.dedup.semantic_dedup import SemanticDeduplicator


@dataclass
class FullDedupReport:
    """Combined hash + semantic dedup report."""

    hash_result: DedupResult
    semantic_result: DedupResult | None = None
    actions_taken: list[str] = field(default_factory=list)

    @property
    def total_duplicates(self) -> int:
        count = self.hash_result.duplicate_count
        if self.semantic_result:
            count += self.semantic_result.duplicate_count
        return count

    def summary(self) -> str:
        lines = [
            "=== Deduplication Report ===",
            f"Hash-based: {self.hash_result.summary()}",
        ]
        if self.semantic_result:
            lines.append(f"Semantic:   {self.semantic_result.summary()}")
        lines.append(f"Total duplicates found: {self.total_duplicates}")
        if self.actions_taken:
            lines.append("\nActions:")
            for a in self.actions_taken:
                lines.append(f"  - {a}")
        return "\n".join(lines)


class DedupEngine:
    """Unified deduplication engine.

    Runs hash-based exact dedup first, then optionally semantic near-dedup
    on remaining items.
    """

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()
        self.hash_dedup = HashDeduplicator(self.config)
        self.semantic_dedup = SemanticDeduplicator(self.config)

    def full_dedup(
        self,
        contents: list[str],
        ids: list[str] | None = None,
        metadatas: list[dict[str, Any]] | None = None,
        embeddings: list[list[float]] | None = None,
        dry_run: bool = True,
    ) -> FullDedupReport:
        """Run full dedup pipeline: hash first, then semantic.

        Args:
            contents: Text contents.
            ids: Item IDs.
            metadatas: Item metadata.
            embeddings: Optional embeddings for semantic dedup.
            dry_run: If True, report only.
        """
        ids = ids or [f"item_{i}" for i in range(len(contents))]
        metadatas = metadatas or [{}] * len(contents)

        # Phase 1: Hash dedup
        hash_result = self.hash_dedup.find_duplicates(contents, ids, metadatas)
        actions: list[str] = []

        if hash_result.duplicate_count > 0:
            actions.append(
                f"Found {hash_result.duplicate_count} exact duplicates in "
                f"{len(hash_result.duplicate_groups)} groups"
            )

        # Phase 2: Semantic dedup on remaining items
        semantic_result = None
        if embeddings is not None:
            remaining_mask = set(hash_result.removed_ids)
            remaining_indices = [
                i for i, item_id in enumerate(ids) if item_id not in remaining_mask
            ]
            if len(remaining_indices) > 1:
                remaining_embeddings = [embeddings[i] for i in remaining_indices]
                remaining_ids = [ids[i] for i in remaining_indices]
                semantic_result = self.semantic_dedup.find_near_duplicates(
                    remaining_embeddings, remaining_ids
                )
                if semantic_result.duplicate_count > 0:
                    actions.append(
                        f"Found {semantic_result.duplicate_count} near-duplicates by embedding similarity"
                    )

        return FullDedupReport(
            hash_result=hash_result,
            semantic_result=semantic_result,
            actions_taken=actions,
        )

    def dedup_chromadb_collection(
        self,
        collection_path: str,
        collection_name: str = "nanobot_kb",
        include_semantic: bool = True,
        dry_run: bool = True,
    ) -> FullDedupReport:
        """Run full dedup on a ChromaDB collection.

        Args:
            collection_path: Path to ChromaDB persist directory.
            collection_name: Collection name.
            include_semantic: Whether to run semantic dedup.
            dry_run: If True, report only — don't delete.
        """
        import chromadb

        client = chromadb.PersistentClient(path=collection_path)
        collection = client.get_collection(collection_name)

        includes = ["documents", "metadatas"]
        if include_semantic:
            includes.append("embeddings")

        data = collection.get(include=includes)
        contents = data.get("documents", [])
        ids = data.get("ids", [])
        metadatas = data.get("metadatas", [])
        embeddings = data.get("embeddings") if include_semantic else None

        report = self.full_dedup(contents, ids, metadatas, embeddings, dry_run=dry_run)

        if not dry_run:
            all_remove_ids = list(report.hash_result.removed_ids)
            if report.semantic_result:
                all_remove_ids.extend(report.semantic_result.removed_ids)

            if all_remove_ids:
                batch_size = self.config.dedup.batch_size
                for i in range(0, len(all_remove_ids), batch_size):
                    batch = all_remove_ids[i : i + batch_size]
                    collection.delete(ids=batch)
                report.actions_taken.append(
                    f"Deleted {len(all_remove_ids)} duplicate chunks from ChromaDB"
                )

        return report
