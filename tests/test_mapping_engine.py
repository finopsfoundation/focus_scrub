"""Tests for the MappingEngine."""

from __future__ import annotations

import re

from focus_scrub.mapping import MappingEngine


class TestMappingEngine:
    """Test the central mapping engine."""

    def test_number_id_mapping_consistency(self, mapping_engine: MappingEngine) -> None:
        """Test that the same number ID always maps to the same value."""
        account_id = "000011112222"

        # First mapping
        result1 = mapping_engine.map_number_id(account_id)

        # Second mapping should return same result
        result2 = mapping_engine.map_number_id(account_id)

        assert result1 == result2
        assert len(result1) == len(account_id)
        assert result1 != account_id
        assert result1.isdigit()

    def test_number_id_preserves_length(self, mapping_engine: MappingEngine) -> None:
        """Test that number ID mapping preserves digit length."""
        test_cases = ["99", "0000000000", "111122223333"]

        for test_id in test_cases:
            result = mapping_engine.map_number_id(test_id)
            assert len(result) == len(test_id)
            assert result.isdigit()

    def test_uuid_mapping_consistency(self, mapping_engine: MappingEngine) -> None:
        """Test that the same UUID always maps to the same value."""
        uuid = "00000000-1111-2222-3333-444444444444"

        result1 = mapping_engine.map_uuid(uuid)
        result2 = mapping_engine.map_uuid(uuid)

        assert result1 == result2
        assert result1 != uuid
        # Verify it's a valid UUID format
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
        )
        assert uuid_pattern.match(result1)

    def test_name_mapping_consistency(self, mapping_engine: MappingEngine) -> None:
        """Test that the same name always maps to the same value."""
        name = "Example Corporation"

        result1 = mapping_engine.map_name(name)
        result2 = mapping_engine.map_name(name)

        assert result1 == result2
        assert result1 != name
        # Should be a stellar-themed name
        assert " " in result1  # Should have format "Term Letter"

    def test_name_mapping_uniqueness(self, mapping_engine: MappingEngine) -> None:
        """Test that different names map to different values."""
        name1 = "Company A"
        name2 = "Company B"

        result1 = mapping_engine.map_name(name1)
        result2 = mapping_engine.map_name(name2)

        assert result1 != result2

    def test_profile_code_mapping_consistency(self, mapping_engine: MappingEngine) -> None:
        """Test that profile codes map consistently."""
        code = "TEST-1234-ABCD"

        result1 = mapping_engine.map_profile_code(code)
        result2 = mapping_engine.map_profile_code(code)

        assert result1 == result2
        assert result1 != code
        # Should preserve structure
        assert len(result1.split("-")) == len(code.split("-"))

    def test_get_all_mappings(self, mapping_engine: MappingEngine) -> None:
        """Test that get_all_mappings returns all mapping types."""
        # Create some mappings
        mapping_engine.map_number_id("000011112222")
        mapping_engine.map_uuid("00000000-1111-2222-3333-444444444444")
        mapping_engine.map_name("Test Company")
        mapping_engine.map_profile_code("TEST-9999")

        all_mappings = mapping_engine.get_all_mappings()

        assert "NumberId" in all_mappings
        assert "UUID" in all_mappings
        assert "Name" in all_mappings
        assert "ProfileCode" in all_mappings

        assert "000011112222" in all_mappings["NumberId"]
        assert "00000000-1111-2222-3333-444444444444" in all_mappings["UUID"]
        assert "Test Company" in all_mappings["Name"]
        assert "TEST-9999" in all_mappings["ProfileCode"]

    def test_load_mappings(self, mapping_engine: MappingEngine) -> None:
        """Test loading mappings from a dictionary."""
        mappings = {
            "NumberId": {"000011112222": "999988887777"},
            "UUID": {
                "00000000-1111-2222-3333-444444444444": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            },
            "Name": {"Test Company": "Nebula Alpha"},
        }

        mapping_engine.load_mappings(mappings)

        # Verify loaded mappings are used
        assert mapping_engine.map_number_id("000011112222") == "999988887777"
        assert (
            mapping_engine.map_uuid("00000000-1111-2222-3333-444444444444")
            == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
        assert mapping_engine.map_name("Test Company") == "Nebula Alpha"

    def test_load_mappings_preserves_new_mappings(self, mapping_engine: MappingEngine) -> None:
        """Test that loading mappings doesn't prevent new mappings from being created."""
        mappings = {"NumberId": {"111111111111": "999999999999"}}

        mapping_engine.load_mappings(mappings)

        # Old mapping should work
        assert mapping_engine.map_number_id("111111111111") == "999999999999"

        # New mappings should still be created
        new_result = mapping_engine.map_number_id("555555555555")
        assert new_result != "555555555555"
        assert len(new_result) == 12
