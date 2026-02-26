"""Mapping collector for tracking old->new value mappings."""

from __future__ import annotations


class MappingCollector:
    """Accumulates old->new mappings reported by handlers across all files."""

    def __init__(self) -> None:
        self._mappings: dict[str, list[tuple[str, str]]] = {}
        self._seen: dict[str, set[str]] = {}

    def record(self, column_name: str, original: str, replacement: str) -> None:
        if column_name not in self._mappings:
            self._mappings[column_name] = []
            self._seen[column_name] = set()
        if original not in self._seen[column_name]:
            self._mappings[column_name].append((original, replacement))
            self._seen[column_name].add(original)

    def to_dict(self) -> dict[str, list[tuple[str, str]]]:
        return {col: list(pairs) for col, pairs in self._mappings.items()}
