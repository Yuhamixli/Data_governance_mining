"""Tests for freshness tracker."""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

from data_governance.core.config import GovernanceConfig
from data_governance.freshness.tracker import FreshnessTracker
from data_governance.freshness.policies import FreshnessPolicy, ExpirationAction


class TestFreshnessPolicy:
    def test_is_stale(self):
        policy = FreshnessPolicy(
            name="test", category="test", stale_days=30, ttl_days=60
        )
        assert not policy.is_stale(10)
        assert policy.is_stale(40)

    def test_is_expired(self):
        policy = FreshnessPolicy(
            name="test", category="test", stale_days=30, ttl_days=60
        )
        assert not policy.is_expired(50)
        assert policy.is_expired(70)

    def test_no_ttl_never_expires(self):
        policy = FreshnessPolicy(
            name="test", category="test", stale_days=30, ttl_days=None
        )
        assert not policy.is_expired(9999)


class TestFreshnessTracker:
    def test_scan_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = FreshnessTracker()
            report = tracker.scan_knowledge_dir(tmpdir)
            assert report.total == 0

    def test_scan_with_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test structure
            long_term = Path(tmpdir) / "长期"
            long_term.mkdir()
            (long_term / "doc.md").write_text("test content", encoding="utf-8")

            short_term = Path(tmpdir) / "短期"
            short_term.mkdir()
            (short_term / "temp.md").write_text("temp content", encoding="utf-8")

            tracker = FreshnessTracker()
            report = tracker.scan_knowledge_dir(tmpdir)

            assert report.total == 2
            categories = {r.category for r in report.records}
            assert "long_term" in categories
            assert "short_term" in categories

    def test_freshness_report_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            long_term = Path(tmpdir) / "长期"
            long_term.mkdir()
            (long_term / "doc.md").write_text("test", encoding="utf-8")

            tracker = FreshnessTracker()
            report = tracker.scan_knowledge_dir(tmpdir)
            summary = report.summary()
            assert "Freshness Report" in summary
