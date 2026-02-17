"""Tests for validation framework."""

import pytest

from data_governance.validation.builtin import BuiltinRules
from data_governance.validation.rules import RuleSet, ValidationRule
from data_governance.core.models import ValidationSeverity


class TestValidationRules:
    def test_chunk_not_empty(self):
        rules = BuiltinRules.knowledge_chunk_rules()
        results = rules.validate(content="valid content here")
        not_empty = next(r for r in results if r.rule_name == "chunk_not_empty")
        assert not_empty.passed

    def test_chunk_empty_fails(self):
        rules = BuiltinRules.knowledge_chunk_rules()
        results = rules.validate(content="")
        not_empty = next(r for r in results if r.rule_name == "chunk_not_empty")
        assert not not_empty.passed

    def test_chunk_min_length(self):
        rules = BuiltinRules.knowledge_chunk_rules()
        results = rules.validate(content="ab", min_length=20)
        min_len = next(r for r in results if r.rule_name == "chunk_min_length")
        assert not min_len.passed

    def test_chunk_no_replacement_chars(self):
        rules = BuiltinRules.knowledge_chunk_rules()
        results = rules.validate(content="clean text")
        no_repl = next(r for r in results if r.rule_name == "chunk_no_replacement_chars")
        assert no_repl.passed

        results = rules.validate(content="corrupted \ufffd text")
        no_repl = next(r for r in results if r.rule_name == "chunk_no_replacement_chars")
        assert not no_repl.passed

    def test_chunk_has_source(self):
        rules = BuiltinRules.knowledge_chunk_rules()
        results = rules.validate(content="text", metadata={"source": "doc.md"})
        has_src = next(r for r in results if r.rule_name == "chunk_has_source")
        assert has_src.passed

        results = rules.validate(content="text", metadata={})
        has_src = next(r for r in results if r.rule_name == "chunk_has_source")
        assert not has_src.passed


class TestCustomRules:
    def test_custom_rule(self):
        rule = ValidationRule(
            name="custom_check",
            description="Test custom rule",
            severity=ValidationSeverity.WARNING,
        ).with_check(
            check_fn=lambda value=0, **kw: value > 10,
            message_fn=lambda value=0, **kw: f"Value {value} is too low",
        )

        result = rule.check(value=5)
        assert not result.passed
        assert "too low" in result.message

        result = rule.check(value=15)
        assert result.passed

    def test_rule_set(self):
        ruleset = RuleSet(name="test", description="Test rules")
        ruleset.add_rule(
            ValidationRule(name="r1", severity=ValidationSeverity.ERROR).with_check(
                check_fn=lambda **kw: True
            )
        )
        ruleset.add_rule(
            ValidationRule(name="r2", severity=ValidationSeverity.WARNING).with_check(
                check_fn=lambda **kw: False
            )
        )

        results = ruleset.validate()
        assert len(results) == 2
        failures = ruleset.get_failures(results)
        assert len(failures) == 1
        assert failures[0].rule_name == "r2"
