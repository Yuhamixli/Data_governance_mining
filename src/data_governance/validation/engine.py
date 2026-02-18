"""Validation execution engine — runs rule sets against data assets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from data_governance.core.config import GovernanceConfig
from data_governance.core.models import ValidationResult, ValidationSeverity
from data_governance.validation.builtin import BuiltinRules
from data_governance.validation.rules import RuleSet


class ValidationEngine:
    """Orchestrates validation across different data asset types.

    Provides high-level methods to validate knowledge bases, chat histories,
    and memory files using appropriate built-in rule sets.
    """

    def __init__(self, config: GovernanceConfig | None = None):
        self.config = config or GovernanceConfig()
        self.rule_sets: dict[str, RuleSet] = {}
        self._register_builtin_rules()

    def _register_builtin_rules(self) -> None:
        """Register all built-in rule sets."""
        self.rule_sets["knowledge_chunks"] = BuiltinRules.knowledge_chunk_rules()
        self.rule_sets["document_files"] = BuiltinRules.document_file_rules()
        self.rule_sets["chat_history"] = BuiltinRules.chat_history_rules()
        self.rule_sets["memory"] = BuiltinRules.memory_rules()

    def register_rule_set(self, rule_set: RuleSet) -> None:
        """Register a custom rule set."""
        self.rule_sets[rule_set.name] = rule_set

    def validate_chunks(
        self,
        chunks: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> list[ValidationResult]:
        """Validate a list of knowledge base chunks.

        Args:
            chunks: List of text contents.
            metadatas: Optional metadata per chunk.
            ids: Optional chunk IDs.
        """
        ruleset = self.rule_sets.get("knowledge_chunks")
        if not ruleset:
            return []

        metadatas = metadatas or [{}] * len(chunks)
        ids = ids or [f"chunk_{i}" for i in range(len(chunks))]
        results: list[ValidationResult] = []

        for i, (content, meta, chunk_id) in enumerate(zip(chunks, metadatas, ids)):
            chunk_results = ruleset.validate(
                content=content,
                metadata=meta,
                min_length=self.config.profiler.min_chunk_length,
                max_length=self.config.profiler.max_chunk_length,
            )
            for r in chunk_results:
                r.asset_id = chunk_id
            results.extend(chunk_results)

            if self.config.validation.fail_on_error:
                failures = [
                    r
                    for r in chunk_results
                    if not r.passed and r.severity == ValidationSeverity.ERROR
                ]
                if failures:
                    break

        return results

    def validate_document_files(self, directory: str | Path) -> list[ValidationResult]:
        """Validate all document files in a directory.

        Args:
            directory: Path to scan for document files.
        """
        ruleset = self.rule_sets.get("document_files")
        if not ruleset:
            return []

        directory = Path(directory)
        results: list[ValidationResult] = []
        supported = {".txt", ".md", ".pdf", ".docx", ".xlsx"}

        for path in sorted(directory.rglob("*")):
            if path.is_file() and path.suffix.lower() in supported:
                file_results = ruleset.validate(file_path=str(path))
                for r in file_results:
                    r.asset_id = str(path)
                results.extend(file_results)

        return results

    def validate_chat_history(self, history_dir: str | Path) -> list[ValidationResult]:
        """Validate chat history JSONL files.

        Args:
            history_dir: Path to chat history directory.
        """
        ruleset = self.rule_sets.get("chat_history")
        if not ruleset:
            return []

        history_dir = Path(history_dir)
        results: list[ValidationResult] = []

        for jsonl_path in sorted(history_dir.rglob("*.jsonl")):
            try:
                lines = jsonl_path.read_text(encoding="utf-8", errors="replace").splitlines()
                for line_num, line in enumerate(lines, 1):
                    if not line.strip():
                        continue

                    # Validate JSON syntax
                    json_results = ruleset.validate(line=line)
                    for r in json_results:
                        r.asset_id = f"{jsonl_path}:{line_num}"

                    # If JSON is valid, also validate message structure
                    try:
                        message = json.loads(line)
                        msg_results = ruleset.validate(message=message)
                        for r in msg_results:
                            r.asset_id = f"{jsonl_path}:{line_num}"
                        json_results.extend(msg_results)
                    except (json.JSONDecodeError, ValueError):
                        pass

                    results.extend(json_results)
            except Exception as e:
                results.append(
                    ValidationResult(
                        rule_name="file_readable",
                        passed=False,
                        severity=ValidationSeverity.ERROR,
                        message=f"Cannot read file: {e}",
                        asset_id=str(jsonl_path),
                    )
                )

        return results

    def validate_memory_files(self, memory_dir: str | Path) -> list[ValidationResult]:
        """Validate agent memory files.

        Args:
            memory_dir: Path to memory directory.
        """
        ruleset = self.rule_sets.get("memory")
        if not ruleset:
            return []

        memory_dir = Path(memory_dir)
        results: list[ValidationResult] = []

        for md_path in sorted(memory_dir.rglob("*.md")):
            try:
                content = md_path.read_text(encoding="utf-8", errors="replace")
                file_results = ruleset.validate(content=content)
                for r in file_results:
                    r.asset_id = str(md_path)
                results.extend(file_results)
            except Exception as e:
                results.append(
                    ValidationResult(
                        rule_name="file_readable",
                        passed=False,
                        severity=ValidationSeverity.ERROR,
                        message=f"Cannot read file: {e}",
                        asset_id=str(md_path),
                    )
                )

        return results

    def validate_chromadb_collection(
        self,
        collection_path: str,
        collection_name: str = "xnobot_kb",
    ) -> list[ValidationResult]:
        """Validate a ChromaDB collection's chunks.

        Args:
            collection_path: Path to ChromaDB persist directory.
            collection_name: Collection name.
        """
        try:
            import chromadb

            client = chromadb.PersistentClient(path=collection_path)
            collection = client.get_collection(collection_name)
            data = collection.get(include=["documents", "metadatas"])

            return self.validate_chunks(
                chunks=data.get("documents", []),
                metadatas=data.get("metadatas", []),
                ids=data.get("ids", []),
            )
        except Exception as e:
            return [
                ValidationResult(
                    rule_name="chromadb_accessible",
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Cannot access ChromaDB: {e}",
                )
            ]

    def summary(self, results: list[ValidationResult]) -> str:
        """Generate a summary of validation results."""
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        errors = sum(
            1
            for r in results
            if not r.passed and r.severity == ValidationSeverity.ERROR
        )
        warnings = sum(
            1
            for r in results
            if not r.passed and r.severity == ValidationSeverity.WARNING
        )
        infos = sum(
            1
            for r in results
            if not r.passed and r.severity == ValidationSeverity.INFO
        )

        lines = [
            f"Validation Summary: {passed}/{total} passed",
            f"  Errors: {errors} | Warnings: {warnings} | Info: {infos}",
        ]

        # Group failures by rule
        failures: dict[str, int] = {}
        for r in results:
            if not r.passed:
                failures[r.rule_name] = failures.get(r.rule_name, 0) + 1

        if failures:
            lines.append("\nFailure breakdown:")
            for rule, count in sorted(failures.items(), key=lambda x: -x[1]):
                lines.append(f"  {rule}: {count} failures")

        return "\n".join(lines)
