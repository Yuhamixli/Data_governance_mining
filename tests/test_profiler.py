"""Tests for data profiler modules."""

import pytest

from data_governance.core.config import GovernanceConfig
from data_governance.profiler.metrics import QualityMetrics
from data_governance.profiler.chunk import ChunkProfiler


class TestQualityMetrics:
    def test_text_length(self):
        assert QualityMetrics.text_length("hello") == 5
        assert QualityMetrics.text_length("") == 0

    def test_word_count(self):
        assert QualityMetrics.word_count("hello world") == 2
        assert QualityMetrics.word_count("") == 0

    def test_char_density(self):
        assert QualityMetrics.char_density("abc") == 1.0
        assert QualityMetrics.char_density("a b c") == pytest.approx(0.6)
        assert QualityMetrics.char_density("") == 0.0

    def test_cjk_ratio(self):
        assert QualityMetrics.cjk_ratio("你好世界") == 1.0
        assert QualityMetrics.cjk_ratio("hello") == 0.0
        assert QualityMetrics.cjk_ratio("") == 0.0

    def test_entropy(self):
        assert QualityMetrics.entropy("") == 0.0
        assert QualityMetrics.entropy("aaaa") < QualityMetrics.entropy("abcd")

    def test_null_ratio(self):
        assert QualityMetrics.null_ratio([]) == 1.0
        assert QualityMetrics.null_ratio(["a", "b"]) == 0.0
        assert QualityMetrics.null_ratio(["a", None, ""]) == pytest.approx(2 / 3)

    def test_duplicate_ratio(self):
        assert QualityMetrics.duplicate_ratio([]) == 0.0
        assert QualityMetrics.duplicate_ratio(["a", "b"]) == 0.0
        assert QualityMetrics.duplicate_ratio(["a", "a"]) == 0.5

    def test_gibberish_score(self):
        assert QualityMetrics.gibberish_score("") == 0.0
        clean = QualityMetrics.gibberish_score("This is normal text about data governance.")
        corrupted = QualityMetrics.gibberish_score("Th\ufffdis i\ufffds corr\ufffdupted")
        assert clean < corrupted

    def test_content_hash(self):
        h1 = QualityMetrics.content_hash("hello")
        h2 = QualityMetrics.content_hash("hello")
        h3 = QualityMetrics.content_hash("world")
        assert h1 == h2
        assert h1 != h3

    def test_completeness_score(self):
        assert QualityMetrics.completeness_score("") == 0.0
        assert QualityMetrics.completeness_score("x", min_length=10) < 1.0
        assert QualityMetrics.completeness_score("hello world here", min_length=5) == 1.0

    def test_freshness_score(self):
        assert QualityMetrics.freshness_score(100, 100, 30) == 1.0
        assert QualityMetrics.freshness_score(0, 86400 * 30, 30) == 0.0


class TestChunkProfiler:
    def setup_method(self):
        self.profiler = ChunkProfiler()

    def test_profile_empty_chunks(self):
        report = self.profiler.profile_chunks([])
        assert report.overall_score == 0.0

    def test_profile_good_chunks(self):
        chunks = [
            "This is a good chunk about data governance and quality management.",
            "Another well-formed chunk discussing validation rules and best practices.",
            "A third chunk explaining deduplication strategies for vector databases.",
        ]
        report = self.profiler.profile_chunks(chunks)
        assert report.overall_score > 0.5
        assert report.total_items == 3

    def test_profile_with_duplicates(self):
        chunks = ["duplicate content here xyz", "duplicate content here xyz", "unique content abc"]
        report = self.profiler.profile_chunks(chunks)
        uniqueness = report.get_dimension_score("uniqueness")
        assert uniqueness is not None
        assert uniqueness < 1.0

    def test_find_problematic_chunks(self):
        chunks = ["", "ab", "good chunk content here for testing", "good chunk content here for testing"]
        problems = self.profiler.find_problematic_chunks(chunks)
        assert len(problems["empty"]) == 1
        assert len(problems["too_short"]) == 1
        assert len(problems["duplicate"]) == 1
