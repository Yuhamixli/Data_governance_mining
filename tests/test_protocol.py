"""Tests for the data quality protocol (passports and quality embedding)."""

import tempfile
from pathlib import Path

import pytest

from data_governance.protocol.data_passport import (
    DataPassport,
    PassportRegistry,
    TrustLevel,
)


class TestDataPassport:
    def test_create_passport(self):
        passport = DataPassport(
            passport_id="pp_test",
            asset_id="chunk:abc",
            content_hash="abcdef1234567890",
            source_type="file",
            source_path="knowledge/doc.md",
        )
        assert passport.trust_level == TrustLevel.UNVERIFIED

    def test_assess_quality(self):
        passport = DataPassport(
            passport_id="pp_test",
            asset_id="chunk:abc",
            content_hash="abcdef1234567890",
            source_type="file",
        )

        # High quality → VERIFIED
        passport.assess(quality_score=0.9, freshness_score=0.8)
        assert passport.trust_level == TrustLevel.VERIFIED
        assert passport.assessment_count == 1

        # Low quality → QUARANTINED
        passport.assess(quality_score=0.1, freshness_score=0.1)
        assert passport.trust_level == TrustLevel.QUARANTINED
        assert passport.assessment_count == 2

    def test_to_metadata(self):
        passport = DataPassport(
            passport_id="pp_test",
            asset_id="chunk:abc",
            content_hash="abcdef1234567890",
            source_type="file",
        )
        passport.assess(quality_score=0.85, freshness_score=0.9)

        meta = passport.to_metadata()
        assert meta["passport_id"] == "pp_test"
        assert meta["quality_score"] == 0.85
        assert meta["trust_level"] == "verified"

    def test_to_agent_context(self):
        passport = DataPassport(
            passport_id="pp_test",
            asset_id="chunk:abc",
            content_hash="abcdef1234567890",
            source_type="file",
        )
        passport.assess(quality_score=0.85, freshness_score=0.9)

        ctx = passport.to_agent_context()
        assert "chunk:abc" in ctx
        assert "verified" in ctx
        assert "85%" in ctx

    def test_record_action(self):
        passport = DataPassport(
            passport_id="pp_test",
            asset_id="chunk:abc",
            content_hash="abcdef1234567890",
            source_type="file",
        )
        passport.record_action("deduplicated", details={"removed_duplicate": "chunk:def"})
        assert len(passport.governance_actions) == 1
        assert passport.governance_actions[0]["action"] == "deduplicated"


class TestPassportRegistry:
    def test_create_and_get(self):
        registry = PassportRegistry()
        passport = registry.create_passport(
            asset_id="chunk:test",
            content="Hello world",
            source_type="file",
        )
        assert passport.passport_id.startswith("pp_")

        retrieved = registry.get_passport("chunk:test")
        assert retrieved is not None
        assert retrieved.asset_id == "chunk:test"

    def test_persistence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "passports.json"

            # Create and save
            reg1 = PassportRegistry(persist_path=path)
            reg1.create_passport("a1", "content1", "file")
            reg1.create_passport("a2", "content2", "web")

            # Load from disk
            reg2 = PassportRegistry(persist_path=path)
            assert len(reg2.passports) == 2
            assert reg2.get_passport("a1") is not None

    def test_get_trusted(self):
        registry = PassportRegistry()
        p1 = registry.create_passport("a1", "content1", "file")
        p2 = registry.create_passport("a2", "content2", "file")
        p3 = registry.create_passport("a3", "content3", "file")

        p1.assess(quality_score=0.9, freshness_score=0.9)  # VERIFIED
        p2.assess(quality_score=0.65, freshness_score=0.5)  # TRUSTED
        p3.assess(quality_score=0.1, freshness_score=0.1)  # QUARANTINED

        trusted = registry.get_trusted(min_trust=TrustLevel.TRUSTED)
        assert len(trusted) == 2

        quarantined = registry.get_quarantined()
        assert len(quarantined) == 1

    def test_stats(self):
        registry = PassportRegistry()
        p1 = registry.create_passport("a1", "content1", "file")
        p1.assess(quality_score=0.9, freshness_score=0.9)

        stats = registry.stats()
        assert stats["total"] == 1
        assert stats["avg_quality"] == 0.9
        assert stats["verified_count"] == 1
