"""Tests for deduplication engine."""

import pytest

from data_governance.core.config import GovernanceConfig
from data_governance.dedup.hash_dedup import HashDeduplicator
from data_governance.dedup.engine import DedupEngine


class TestHashDeduplicator:
    def setup_method(self):
        self.dedup = HashDeduplicator()

    def test_no_duplicates(self):
        contents = ["alpha", "beta", "gamma"]
        result = self.dedup.find_duplicates(contents)
        assert result.duplicate_count == 0
        assert result.unique_items == 3

    def test_exact_duplicates(self):
        contents = ["alpha", "beta", "alpha", "gamma", "beta"]
        result = self.dedup.find_duplicates(contents)
        assert result.duplicate_count == 2
        assert result.unique_items == 3
        assert len(result.duplicate_groups) == 2

    def test_whitespace_normalization(self):
        contents = ["hello  world", "hello world", "hello\tworld"]
        result = self.dedup.find_duplicates(contents, normalize=True)
        assert result.duplicate_count == 2
        assert result.unique_items == 1

    def test_no_normalization(self):
        contents = ["hello  world", "hello world"]
        result = self.dedup.find_duplicates(contents, normalize=False)
        assert result.duplicate_count == 0

    def test_keep_strategy(self):
        contents = ["same", "same"]
        ids = ["id1", "id2"]
        metadatas = [{"source": "a.md"}, {}]
        result = self.dedup.find_duplicates(contents, ids, metadatas)
        # Should keep the one with more metadata
        assert result.duplicate_groups[0].keep_id == "id1"
        assert result.duplicate_groups[0].remove_ids == ["id2"]

    def test_empty_input(self):
        result = self.dedup.find_duplicates([])
        assert result.total_items == 0
        assert result.duplicate_count == 0


class TestDedupEngine:
    def setup_method(self):
        self.engine = DedupEngine()

    def test_full_dedup_hash_only(self):
        contents = ["a", "b", "a", "c"]
        report = self.engine.full_dedup(contents)
        assert report.hash_result.duplicate_count == 1
        assert report.semantic_result is None

    def test_summary_output(self):
        contents = ["x", "y", "x"]
        report = self.engine.full_dedup(contents)
        summary = report.summary()
        assert "Deduplication Report" in summary
        assert "Hash-based" in summary
