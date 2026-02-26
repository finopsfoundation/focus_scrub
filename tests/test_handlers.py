"""Tests for column handlers."""

from __future__ import annotations

import pandas as pd

from focus_scrub.handlers import AccountIdHandler, CommitmentDiscountIdHandler, StellarNameHandler
from focus_scrub.mapping import MappingCollector, MappingEngine


class TestAccountIdHandler:
    """Test the AccountIdHandler."""

    def test_pure_numeric_id(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing pure numeric account IDs."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)

        account_id = "000011112222"
        result1 = handler.scrub(account_id)
        result2 = handler.scrub(account_id)

        assert result1 == result2
        assert len(result1) == len(account_id)
        assert result1.isdigit()
        assert result1 != account_id

    def test_arn_with_account_id(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing ARN with embedded account ID."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)

        arn = "arn:aws:ec2:us-east-1:000011112222:reserved-instances/00000000-1111-2222-3333-444444444444"
        result = handler.scrub(arn)

        # Should still be an ARN
        assert result.startswith("arn:aws:ec2:us-east-1:")
        assert ":reserved-instances/" in result

        # Account ID should be mapped
        assert "000011112222" not in result

        # UUID should be mapped
        assert "00000000-1111-2222-3333-444444444444" not in result

    def test_arn_consistency_across_columns(self, mapping_engine: MappingEngine) -> None:
        """Test that account ID in ARN maps consistently with standalone account ID."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)

        account_id = "333344445555"
        arn = f"arn:aws:ec2:us-east-1:{account_id}:reserved-instances/00000000-1111-2222-3333-444444444444"

        # Map standalone account ID first
        mapped_account = handler.scrub(account_id)

        # Map ARN
        mapped_arn = handler.scrub(arn)

        # Account ID in ARN should match standalone mapping
        assert mapped_account in mapped_arn

    def test_na_values_passed_through(self, mapping_engine: MappingEngine) -> None:
        """Test that NA values are passed through unchanged."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)

        result = handler.scrub(pd.NA)
        assert pd.isna(result)

        result = handler.scrub(None)
        assert pd.isna(result)

    def test_collector_records_mappings(self, mapping_engine: MappingEngine) -> None:
        """Test that handler records mappings to collector."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)
        collector = MappingCollector()
        handler.attach_collector("TestColumn", collector)

        original = "777788889999"
        result = handler.scrub(original)

        mappings = collector.to_dict()
        assert "TestColumn" in mappings
        assert len(mappings["TestColumn"]) == 1
        assert mappings["TestColumn"][0] == (original, result)


class TestStellarNameHandler:
    """Test the StellarNameHandler."""

    def test_name_mapping_consistency(self, mapping_engine: MappingEngine) -> None:
        """Test that names map consistently."""
        handler = StellarNameHandler(mapping_engine=mapping_engine)

        name = "Example Corporation"
        result1 = handler.scrub(name)
        result2 = handler.scrub(name)

        assert result1 == result2
        assert result1 != name

    def test_stellar_name_format(self, mapping_engine: MappingEngine) -> None:
        """Test that generated names follow stellar format."""
        handler = StellarNameHandler(mapping_engine=mapping_engine)

        name = "Test Company"
        result = handler.scrub(name)

        # Should have format "Term Letter" or "Term Letter N"
        parts = result.split()
        assert len(parts) >= 2

    def test_unique_names(self, mapping_engine: MappingEngine) -> None:
        """Test that different input names get different stellar names."""
        handler = StellarNameHandler(mapping_engine=mapping_engine)

        names = ["Company A", "Company B", "Company C"]
        results = [handler.scrub(name) for name in names]

        # All results should be unique
        assert len(results) == len(set(results))


class TestCommitmentDiscountIdHandler:
    """Test the CommitmentDiscountIdHandler."""

    def test_delegates_to_account_id_handler(self, mapping_engine: MappingEngine) -> None:
        """Test that CommitmentDiscountIdHandler delegates to AccountIdHandler."""
        handler = CommitmentDiscountIdHandler(mapping_engine=mapping_engine)

        account_id = "666677778888"
        result = handler.scrub(account_id)

        assert result != account_id
        assert len(result) == len(account_id)
        assert result.isdigit()

    def test_shares_mapping_engine(self, mapping_engine: MappingEngine) -> None:
        """Test that CommitmentDiscountIdHandler shares mappings with AccountIdHandler."""
        account_handler = AccountIdHandler(mapping_engine=mapping_engine)
        commitment_handler = CommitmentDiscountIdHandler(mapping_engine=mapping_engine)

        account_id = "333344445555"

        # Map via account handler
        account_result = account_handler.scrub(account_id)

        # Map via commitment handler - should get same result
        commitment_result = commitment_handler.scrub(account_id)

        assert account_result == commitment_result
