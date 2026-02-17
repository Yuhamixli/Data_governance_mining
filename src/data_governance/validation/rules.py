"""Declarative validation rule definitions."""

from __future__ import annotations

from typing import Any, Callable

from pydantic import BaseModel, Field

from data_governance.core.models import ValidationResult, ValidationSeverity


class ValidationRule(BaseModel):
    """A single validation rule.

    Rules are declarative checks that can be applied to data assets.
    Each rule has a check function, severity, and descriptive metadata.
    """

    name: str = Field(description="Unique rule name")
    description: str = Field(default="", description="Human-readable description")
    severity: ValidationSeverity = Field(default=ValidationSeverity.ERROR)
    category: str = Field(default="general", description="Rule category for grouping")
    enabled: bool = Field(default=True)

    # The check function is not serialized — set via .with_check()
    _check_fn: Callable[..., bool] | None = None
    _message_fn: Callable[..., str] | None = None

    model_config = {"arbitrary_types_allowed": True}

    def with_check(
        self,
        check_fn: Callable[..., bool],
        message_fn: Callable[..., str] | None = None,
    ) -> ValidationRule:
        """Attach a check function to this rule.

        Args:
            check_fn: Function that returns True if the check passes.
            message_fn: Optional function that returns a failure message.
        """
        self._check_fn = check_fn
        self._message_fn = message_fn
        return self

    def check(self, **kwargs: Any) -> ValidationResult:
        """Execute the validation check.

        Args:
            **kwargs: Arguments passed to the check function.
        """
        if not self.enabled:
            return ValidationResult(
                rule_name=self.name,
                passed=True,
                severity=self.severity,
                message="Rule disabled",
            )

        if self._check_fn is None:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="No check function configured",
            )

        try:
            passed = self._check_fn(**kwargs)
            message = ""
            if not passed and self._message_fn:
                message = self._message_fn(**kwargs)
            elif not passed:
                message = f"Validation failed: {self.name}"
            return ValidationResult(
                rule_name=self.name,
                passed=passed,
                severity=self.severity,
                message=message,
                details=kwargs,
            )
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Rule execution error: {e}",
                details={"error": str(e)},
            )


class RuleSet(BaseModel):
    """A named collection of validation rules."""

    name: str = Field(description="Rule set name")
    description: str = Field(default="")
    rules: list[ValidationRule] = Field(default_factory=list)

    model_config = {"arbitrary_types_allowed": True}

    def add_rule(self, rule: ValidationRule) -> RuleSet:
        self.rules.append(rule)
        return self

    def validate(self, **kwargs: Any) -> list[ValidationResult]:
        """Run all enabled rules and return results."""
        results: list[ValidationResult] = []
        for rule in self.rules:
            if rule.enabled:
                results.append(rule.check(**kwargs))
        return results

    def get_failures(self, results: list[ValidationResult]) -> list[ValidationResult]:
        return [r for r in results if not r.passed]

    def summary(self, results: list[ValidationResult]) -> str:
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failed = total - passed
        lines = [f"Rule Set: {self.name} — {passed}/{total} passed, {failed} failed"]
        for r in results:
            status = "PASS" if r.passed else "FAIL"
            severity = f"[{r.severity.value}]" if not r.passed else ""
            lines.append(f"  {status} {severity} {r.rule_name}: {r.message}")
        return "\n".join(lines)
