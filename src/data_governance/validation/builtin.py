"""Built-in validation rules for common data quality checks."""

from __future__ import annotations

import re
from pathlib import Path

from data_governance.core.models import ValidationSeverity
from data_governance.validation.rules import RuleSet, ValidationRule


class BuiltinRules:
    """Factory for built-in validation rule sets.

    Provides pre-configured rules for knowledge base documents, chunks,
    chat history, and memory files — tailored for xnobot's data types.
    """

    @staticmethod
    def knowledge_chunk_rules() -> RuleSet:
        """Rules for validating knowledge base chunks."""
        ruleset = RuleSet(
            name="knowledge_chunks",
            description="Validation rules for knowledge base text chunks",
        )

        ruleset.add_rule(
            ValidationRule(
                name="chunk_not_empty",
                description="Chunk content must not be empty",
                severity=ValidationSeverity.ERROR,
                category="completeness",
            ).with_check(
                check_fn=lambda content="", **kw: bool(content and content.strip()),
                message_fn=lambda **kw: "Chunk is empty or whitespace-only",
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="chunk_min_length",
                description="Chunk must meet minimum length",
                severity=ValidationSeverity.WARNING,
                category="completeness",
            ).with_check(
                check_fn=lambda content="", min_length=20, **kw: len(content.strip()) >= min_length,
                message_fn=lambda content="", min_length=20, **kw: (
                    f"Chunk too short: {len(content.strip())} chars (min: {min_length})"
                ),
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="chunk_max_length",
                description="Chunk must not exceed maximum length",
                severity=ValidationSeverity.WARNING,
                category="consistency",
            ).with_check(
                check_fn=lambda content="", max_length=5000, **kw: len(content) <= max_length,
                message_fn=lambda content="", max_length=5000, **kw: (
                    f"Chunk too long: {len(content)} chars (max: {max_length})"
                ),
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="chunk_no_replacement_chars",
                description="No Unicode replacement characters (encoding issues)",
                severity=ValidationSeverity.WARNING,
                category="validity",
            ).with_check(
                check_fn=lambda content="", **kw: "\ufffd" not in content,
                message_fn=lambda content="", **kw: (
                    f"Found {content.count(chr(0xFFFD))} replacement characters"
                ),
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="chunk_no_control_chars",
                description="No unexpected control characters",
                severity=ValidationSeverity.WARNING,
                category="validity",
            ).with_check(
                check_fn=lambda content="", **kw: not re.search(
                    r"[\x00-\x08\x0b\x0c\x0e-\x1f]", content
                ),
                message_fn=lambda **kw: "Control characters found in content",
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="chunk_has_source",
                description="Chunk metadata must include source path",
                severity=ValidationSeverity.WARNING,
                category="completeness",
            ).with_check(
                check_fn=lambda metadata=None, **kw: bool(
                    metadata and metadata.get("source")
                ),
                message_fn=lambda **kw: "Missing source path in metadata",
            )
        )

        return ruleset

    @staticmethod
    def document_file_rules() -> RuleSet:
        """Rules for validating document files before ingestion."""
        ruleset = RuleSet(
            name="document_files",
            description="Validation rules for document files",
        )

        ruleset.add_rule(
            ValidationRule(
                name="file_exists",
                description="File must exist on disk",
                severity=ValidationSeverity.ERROR,
                category="completeness",
            ).with_check(
                check_fn=lambda file_path="", **kw: Path(file_path).exists(),
                message_fn=lambda file_path="", **kw: f"File not found: {file_path}",
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="file_not_empty",
                description="File must not be empty",
                severity=ValidationSeverity.ERROR,
                category="completeness",
            ).with_check(
                check_fn=lambda file_path="", **kw: (
                    Path(file_path).exists() and Path(file_path).stat().st_size > 0
                ),
                message_fn=lambda file_path="", **kw: f"File is empty: {file_path}",
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="file_supported_format",
                description="File must be a supported format",
                severity=ValidationSeverity.ERROR,
                category="validity",
            ).with_check(
                check_fn=lambda file_path="", **kw: Path(file_path).suffix.lower()
                in {".txt", ".md", ".pdf", ".docx", ".xlsx"},
                message_fn=lambda file_path="", **kw: (
                    f"Unsupported format: {Path(file_path).suffix}"
                ),
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="file_size_reasonable",
                description="File size should be reasonable (< 50MB)",
                severity=ValidationSeverity.WARNING,
                category="validity",
            ).with_check(
                check_fn=lambda file_path="", **kw: (
                    Path(file_path).exists()
                    and Path(file_path).stat().st_size < 50 * 1024 * 1024
                ),
                message_fn=lambda file_path="", **kw: "File is larger than 50MB",
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="file_utf8_readable",
                description="Text file should be valid UTF-8",
                severity=ValidationSeverity.WARNING,
                category="validity",
            ).with_check(
                check_fn=lambda file_path="", **kw: _check_utf8(file_path),
                message_fn=lambda file_path="", **kw: "File is not valid UTF-8",
            )
        )

        return ruleset

    @staticmethod
    def chat_history_rules() -> RuleSet:
        """Rules for validating chat history JSONL files."""
        ruleset = RuleSet(
            name="chat_history",
            description="Validation rules for chat history files",
        )

        ruleset.add_rule(
            ValidationRule(
                name="jsonl_valid",
                description="Each line must be valid JSON",
                severity=ValidationSeverity.ERROR,
                category="validity",
            ).with_check(
                check_fn=lambda line="", **kw: _check_json(line),
                message_fn=lambda line="", **kw: "Invalid JSON line",
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="message_has_role",
                description="Message must have a role field",
                severity=ValidationSeverity.ERROR,
                category="completeness",
            ).with_check(
                check_fn=lambda message=None, **kw: bool(
                    message and "role" in message
                ),
                message_fn=lambda **kw: "Message missing 'role' field",
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="message_has_content",
                description="Message must have content",
                severity=ValidationSeverity.WARNING,
                category="completeness",
            ).with_check(
                check_fn=lambda message=None, **kw: bool(
                    message and (message.get("content") or message.get("text"))
                ),
                message_fn=lambda **kw: "Message has no content",
            )
        )

        return ruleset

    @staticmethod
    def memory_rules() -> RuleSet:
        """Rules for validating memory files."""
        ruleset = RuleSet(
            name="memory",
            description="Validation rules for agent memory files",
        )

        ruleset.add_rule(
            ValidationRule(
                name="memory_not_empty",
                description="Memory file should have content",
                severity=ValidationSeverity.WARNING,
                category="completeness",
            ).with_check(
                check_fn=lambda content="", **kw: bool(content and content.strip()),
                message_fn=lambda **kw: "Memory file is empty",
            )
        )

        ruleset.add_rule(
            ValidationRule(
                name="memory_has_structure",
                description="Memory file should have markdown structure",
                severity=ValidationSeverity.INFO,
                category="consistency",
            ).with_check(
                check_fn=lambda content="", **kw: bool(
                    content and re.search(r"^#+\s", content, re.MULTILINE)
                ),
                message_fn=lambda **kw: "Memory file lacks markdown headers",
            )
        )

        return ruleset


def _check_utf8(file_path: str) -> bool:
    """Check if a file is valid UTF-8."""
    if not file_path or not Path(file_path).exists():
        return True  # Skip non-existent files
    suffix = Path(file_path).suffix.lower()
    if suffix not in (".txt", ".md"):
        return True  # Only check text files
    try:
        Path(file_path).read_text(encoding="utf-8")
        return True
    except UnicodeDecodeError:
        return False


def _check_json(line: str) -> bool:
    """Check if a string is valid JSON."""
    import json

    if not line or not line.strip():
        return True  # Empty lines are OK
    try:
        json.loads(line)
        return True
    except (json.JSONDecodeError, ValueError):
        return False
