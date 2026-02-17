"""Data lineage tracking — records how data flows through the system."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from data_governance.core.models import LineageEdge, LineageGraph, LineageNode


class LineageTracker:
    """Track data lineage across the knowledge base pipeline.

    Records the flow of data from source files through chunking, embedding,
    and storage in the vector database. Enables impact analysis and
    root cause tracing.
    """

    def __init__(self, persist_path: str | Path | None = None):
        self.graph = LineageGraph()
        self.persist_path = Path(persist_path) if persist_path else None
        if self.persist_path and self.persist_path.exists():
            self._load()

    def record_ingestion(
        self,
        source_path: str,
        chunk_ids: list[str],
        collection_name: str = "nanobot_kb",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record the lineage of a document ingestion.

        Args:
            source_path: Path to the source document.
            chunk_ids: IDs of the chunks created from this document.
            collection_name: Target collection name.
            metadata: Additional metadata about the ingestion.
        """
        source_node = LineageNode(
            id=f"file:{source_path}",
            name=Path(source_path).name,
            node_type="source",
            metadata={"path": source_path, **(metadata or {})},
        )
        self.graph.add_node(source_node)

        collection_node = LineageNode(
            id=f"collection:{collection_name}",
            name=collection_name,
            node_type="storage",
            metadata={"type": "chromadb"},
        )
        self.graph.add_node(collection_node)

        for chunk_id in chunk_ids:
            chunk_node = LineageNode(
                id=f"chunk:{chunk_id}",
                name=f"Chunk {chunk_id}",
                node_type="transform",
                metadata={"source": source_path},
            )
            self.graph.add_node(chunk_node)

            self.graph.add_edge(
                LineageEdge(
                    source_id=source_node.id,
                    target_id=chunk_node.id,
                    operation="chunk",
                    metadata={"timestamp": datetime.now().isoformat()},
                )
            )
            self.graph.add_edge(
                LineageEdge(
                    source_id=chunk_node.id,
                    target_id=collection_node.id,
                    operation="embed_store",
                )
            )

        self._save()

    def record_web_cache(
        self,
        url: str,
        cache_path: str,
        collection_name: str = "nanobot_kb_web_cache",
    ) -> None:
        """Record lineage for web cache data.

        Args:
            url: Source URL.
            cache_path: Path where the cache file was saved.
            collection_name: Target collection name.
        """
        url_node = LineageNode(
            id=f"url:{url}",
            name=url,
            node_type="source",
            metadata={"type": "web"},
        )
        self.graph.add_node(url_node)

        cache_node = LineageNode(
            id=f"cache:{cache_path}",
            name=Path(cache_path).name,
            node_type="transform",
            metadata={"path": cache_path},
        )
        self.graph.add_node(cache_node)

        collection_node = LineageNode(
            id=f"collection:{collection_name}",
            name=collection_name,
            node_type="storage",
        )
        self.graph.add_node(collection_node)

        self.graph.add_edge(
            LineageEdge(source_id=url_node.id, target_id=cache_node.id, operation="fetch_cache")
        )
        self.graph.add_edge(
            LineageEdge(
                source_id=cache_node.id, target_id=collection_node.id, operation="ingest"
            )
        )

        self._save()

    def record_chat_history_export(
        self,
        chat_file: str,
        qa_pairs: list[str],
        target_path: str,
    ) -> None:
        """Record lineage for chat history -> Q&A pair export.

        Args:
            chat_file: Source chat history file.
            qa_pairs: IDs of generated Q&A pairs.
            target_path: Where the Q&A pairs were saved.
        """
        source_node = LineageNode(
            id=f"chat:{chat_file}",
            name=Path(chat_file).name,
            node_type="source",
            metadata={"type": "chat_history"},
        )
        self.graph.add_node(source_node)

        target_node = LineageNode(
            id=f"file:{target_path}",
            name=Path(target_path).name,
            node_type="transform",
            metadata={"type": "qa_export"},
        )
        self.graph.add_node(target_node)

        self.graph.add_edge(
            LineageEdge(
                source_id=source_node.id,
                target_id=target_node.id,
                operation="export_qa",
                metadata={"qa_count": len(qa_pairs)},
            )
        )

        self._save()

    def get_source_lineage(self, asset_id: str) -> list[LineageNode]:
        """Trace upstream lineage: where did this data come from?"""
        return self.graph.get_lineage_chain(asset_id, direction="upstream")

    def get_impact_analysis(self, asset_id: str) -> list[LineageNode]:
        """Trace downstream impact: what depends on this data?"""
        return self.graph.get_lineage_chain(asset_id, direction="downstream")

    def get_orphaned_chunks(self, existing_sources: set[str]) -> list[LineageNode]:
        """Find chunks whose source files no longer exist.

        Args:
            existing_sources: Set of currently existing source file paths.
        """
        orphaned = []
        for node in self.graph.nodes:
            if node.node_type == "transform":
                source_path = node.metadata.get("source", "")
                if source_path and source_path not in existing_sources:
                    orphaned.append(node)
        return orphaned

    def export_mermaid(self) -> str:
        """Export the lineage graph as Mermaid diagram."""
        return self.graph.to_mermaid()

    def stats(self) -> dict[str, int]:
        """Get lineage graph statistics."""
        node_types: dict[str, int] = {}
        for node in self.graph.nodes:
            node_types[node.node_type] = node_types.get(node.node_type, 0) + 1

        edge_types: dict[str, int] = {}
        for edge in self.graph.edges:
            edge_types[edge.operation] = edge_types.get(edge.operation, 0) + 1

        return {
            "total_nodes": len(self.graph.nodes),
            "total_edges": len(self.graph.edges),
            **{f"nodes_{k}": v for k, v in node_types.items()},
            **{f"edges_{k}": v for k, v in edge_types.items()},
        }

    def _save(self) -> None:
        """Persist lineage graph to disk."""
        if not self.persist_path:
            return
        self.persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "nodes": [n.model_dump() for n in self.graph.nodes],
            "edges": [e.model_dump(mode="json") for e in self.graph.edges],
        }
        with open(self.persist_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    def _load(self) -> None:
        """Load lineage graph from disk."""
        try:
            data = json.loads(self.persist_path.read_text(encoding="utf-8"))
            self.graph = LineageGraph(
                nodes=[LineageNode(**n) for n in data.get("nodes", [])],
                edges=[LineageEdge(**e) for e in data.get("edges", [])],
            )
        except (json.JSONDecodeError, KeyError, TypeError):
            self.graph = LineageGraph()
