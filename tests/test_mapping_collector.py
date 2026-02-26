"""Tests for the MappingCollector."""

from __future__ import annotations

from focus_scrub.mapping import MappingCollector


class TestMappingCollector:
    """Test the mapping collector."""

    def test_record_mapping(self) -> None:
        """Test recording a mapping."""
        collector = MappingCollector()

        collector.record("TestColumn", "original", "replacement")

        mappings = collector.to_dict()
        assert "TestColumn" in mappings
        assert len(mappings["TestColumn"]) == 1
        assert mappings["TestColumn"][0] == ("original", "replacement")

    def test_record_multiple_mappings(self) -> None:
        """Test recording multiple mappings for same column."""
        collector = MappingCollector()

        collector.record("TestColumn", "value1", "mapped1")
        collector.record("TestColumn", "value2", "mapped2")
        collector.record("TestColumn", "value3", "mapped3")

        mappings = collector.to_dict()
        assert len(mappings["TestColumn"]) == 3

    def test_deduplication(self) -> None:
        """Test that duplicate mappings are not recorded."""
        collector = MappingCollector()

        collector.record("TestColumn", "value1", "mapped1")
        collector.record("TestColumn", "value1", "mapped1")
        collector.record("TestColumn", "value1", "mapped1")

        mappings = collector.to_dict()
        assert len(mappings["TestColumn"]) == 1

    def test_multiple_columns(self) -> None:
        """Test recording mappings for multiple columns."""
        collector = MappingCollector()

        collector.record("Column1", "val1", "map1")
        collector.record("Column2", "val2", "map2")
        collector.record("Column3", "val3", "map3")

        mappings = collector.to_dict()
        assert len(mappings) == 3
        assert "Column1" in mappings
        assert "Column2" in mappings
        assert "Column3" in mappings
