"""Core data models for the governance framework."""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DataAssetType(str, Enum):
    """Types of data assets managed by governance."""

    DOCUMENT = "document"
    CHUNK = "chunk"
    COLLECTION = "collection"
    CHAT_HISTORY = "chat_history"
    MEMORY = "memory"
    WEB_CACHE = "web_cache"
    SESSION = "session"
    CONFIG = "config"


class QualityDimension(str, Enum):
    """Standard data quality dimensions."""

    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class DataAsset(BaseModel):
    """Represents a managed data asset."""

    id: str = Field(description="Unique identifier for the asset")
    name: str = Field(description="Human-readable name")
    asset_type: DataAssetType = Field(description="Type of data asset")
    source_path: str | None = Field(default=None, description="Source file path")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    content_hash: str | None = Field(default=None, description="Content hash for change detection")

    def compute_hash(self, content: str | bytes) -> str:
        """Compute content hash for the asset."""
        if isinstance(content, str):
            content = content.encode("utf-8")
        self.content_hash = hashlib.sha256(content).hexdigest()[:16]
        return self.content_hash


class QualityScore(BaseModel):
    """Quality score for a specific dimension."""

    dimension: QualityDimension
    score: float = Field(ge=0.0, le=1.0, description="Score between 0 and 1")
    details: str = Field(default="", description="Human-readable explanation")
    metrics: dict[str, Any] = Field(default_factory=dict, description="Raw metrics")
    measured_at: datetime = Field(default_factory=datetime.now)


class QualityReport(BaseModel):
    """Quality report for a data asset or collection."""

    asset_id: str
    asset_name: str
    asset_type: DataAssetType
    overall_score: float = Field(ge=0.0, le=1.0)
    dimension_scores: list[QualityScore] = Field(default_factory=list)
    total_items: int = 0
    issues_found: int = 0
    recommendations: list[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.now)

    def get_dimension_score(self, dimension: QualityDimension) -> float | None:
        """Get score for a specific quality dimension."""
        for score in self.dimension_scores:
            if score.dimension == dimension:
                return score.score
        return None

    def to_summary(self) -> str:
        """Generate a human-readable summary."""
        lines = [
            f"Quality Report: {self.asset_name} ({self.asset_type.value})",
            f"Overall Score: {self.overall_score:.1%}",
            f"Items: {self.total_items} | Issues: {self.issues_found}",
            "",
        ]
        for ds in self.dimension_scores:
            status = "✓" if ds.score >= 0.8 else "⚠" if ds.score >= 0.6 else "✗"
            lines.append(f"  {status} {ds.dimension.value}: {ds.score:.1%} - {ds.details}")
        if self.recommendations:
            lines.append("")
            lines.append("Recommendations:")
            for rec in self.recommendations:
                lines.append(f"  • {rec}")
        return "\n".join(lines)


class ValidationResult(BaseModel):
    """Result of a single validation check."""

    rule_name: str
    passed: bool
    severity: ValidationSeverity = ValidationSeverity.ERROR
    message: str = ""
    asset_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    checked_at: datetime = Field(default_factory=datetime.now)


class LineageNode(BaseModel):
    """A node in the data lineage graph."""

    id: str
    name: str
    node_type: str  # source, transform, storage, consumer
    metadata: dict[str, Any] = Field(default_factory=dict)


class LineageEdge(BaseModel):
    """An edge in the data lineage graph."""

    source_id: str
    target_id: str
    operation: str  # ingest, chunk, embed, search, cache, etc.
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)


class LineageGraph(BaseModel):
    """Data lineage graph tracking data flow."""

    nodes: list[LineageNode] = Field(default_factory=list)
    edges: list[LineageEdge] = Field(default_factory=list)

    def add_node(self, node: LineageNode) -> None:
        if not any(n.id == node.id for n in self.nodes):
            self.nodes.append(node)

    def add_edge(self, edge: LineageEdge) -> None:
        self.edges.append(edge)

    def get_upstream(self, node_id: str) -> list[LineageNode]:
        """Get all upstream nodes (data sources)."""
        upstream_ids = {e.source_id for e in self.edges if e.target_id == node_id}
        return [n for n in self.nodes if n.id in upstream_ids]

    def get_downstream(self, node_id: str) -> list[LineageNode]:
        """Get all downstream nodes (data consumers)."""
        downstream_ids = {e.target_id for e in self.edges if e.source_id == node_id}
        return [n for n in self.nodes if n.id in downstream_ids]

    def get_lineage_chain(self, node_id: str, direction: str = "upstream") -> list[LineageNode]:
        """Get full lineage chain (recursive)."""
        visited: set[str] = set()
        result: list[LineageNode] = []

        def _traverse(nid: str) -> None:
            if nid in visited:
                return
            visited.add(nid)
            nodes = self.get_upstream(nid) if direction == "upstream" else self.get_downstream(nid)
            for node in nodes:
                result.append(node)
                _traverse(node.id)

        _traverse(node_id)
        return result

    def to_mermaid(self) -> str:
        """Export as Mermaid graph definition."""
        lines = ["graph LR"]
        for node in self.nodes:
            label = node.name.replace('"', '\\"')
            lines.append(f'    {node.id}["{label}"]')
        for edge in self.edges:
            label = edge.operation.replace('"', '\\"')
            lines.append(f'    {edge.source_id} -->|"{label}"| {edge.target_id}')
        return "\n".join(lines)
