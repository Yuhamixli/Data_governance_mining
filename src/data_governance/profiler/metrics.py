"""Quality metric computation utilities."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any


class QualityMetrics:
    """Compute various data quality metrics for text content."""

    @staticmethod
    def text_length(text: str) -> int:
        return len(text)

    @staticmethod
    def word_count(text: str) -> int:
        return len(text.split())

    @staticmethod
    def char_density(text: str) -> float:
        """Ratio of non-whitespace to total characters."""
        if not text:
            return 0.0
        non_ws = len(text.replace(" ", "").replace("\t", "").replace("\n", ""))
        return non_ws / len(text)

    @staticmethod
    def cjk_ratio(text: str) -> float:
        """Ratio of CJK characters (Chinese/Japanese/Korean) to total."""
        if not text:
            return 0.0
        cjk_chars = len(re.findall(r"[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]", text))
        return cjk_chars / len(text) if text else 0.0

    @staticmethod
    def entropy(text: str) -> float:
        """Shannon entropy of character distribution — low entropy = repetitive content."""
        import math

        if not text:
            return 0.0
        freq = Counter(text)
        total = len(text)
        return -sum((c / total) * math.log2(c / total) for c in freq.values())

    @staticmethod
    def null_ratio(values: list[Any]) -> float:
        """Ratio of null/empty values in a list."""
        if not values:
            return 1.0
        nulls = sum(1 for v in values if v is None or v == "" or v == [])
        return nulls / len(values)

    @staticmethod
    def duplicate_ratio(values: list[str]) -> float:
        """Ratio of duplicate values in a list."""
        if not values:
            return 0.0
        unique = set(values)
        return 1.0 - len(unique) / len(values)

    @staticmethod
    def gibberish_score(text: str) -> float:
        """Heuristic score for gibberish/corrupted text (0 = clean, 1 = gibberish).

        Checks for common corruption patterns: replacement chars, control chars,
        excessive special characters, very low entropy.
        """
        if not text:
            return 0.0

        scores: list[float] = []

        replacement_ratio = text.count("\ufffd") / len(text) if text else 0.0
        scores.append(min(replacement_ratio * 10, 1.0))

        control_chars = len(re.findall(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", text))
        scores.append(min(control_chars / max(len(text), 1) * 10, 1.0))

        special_ratio = len(re.findall(r"[^\w\s\u4e00-\u9fff.,;:!?()\"'，。；：！？（）""''、]", text))
        scores.append(min(special_ratio / max(len(text), 1) * 5, 1.0))

        return sum(scores) / len(scores) if scores else 0.0

    @staticmethod
    def freshness_score(updated_at_ts: float, now_ts: float, ttl_days: int = 30) -> float:
        """Score based on how fresh the data is (1.0 = just updated, 0.0 = expired)."""
        age_days = (now_ts - updated_at_ts) / 86400
        if age_days <= 0:
            return 1.0
        if age_days >= ttl_days:
            return 0.0
        return 1.0 - (age_days / ttl_days)

    @staticmethod
    def content_hash(text: str) -> str:
        """Compute a fast content hash for dedup."""
        import xxhash

        return xxhash.xxh64(text.encode("utf-8")).hexdigest()

    @staticmethod
    def completeness_score(
        text: str, min_length: int = 20, expected_sections: list[str] | None = None
    ) -> float:
        """Score text completeness (length, structure, expected content)."""
        if not text or not text.strip():
            return 0.0

        score = 1.0

        length = len(text.strip())
        if length < min_length:
            score *= length / min_length

        if expected_sections:
            found = sum(1 for s in expected_sections if s.lower() in text.lower())
            score *= found / len(expected_sections)

        return min(score, 1.0)
