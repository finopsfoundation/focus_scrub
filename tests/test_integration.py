"""Integration tests for the complete scrubbing workflow."""

from __future__ import annotations

import pandas as pd
from focus_scrub.handlers import HandlerConfig, get_column_handlers_for_dataset
from focus_scrub.mapping import MappingCollector
from focus_scrub.scrub import DataFrameScrub


class TestIntegration:
    """Integration tests for complete workflow."""

    def test_cost_and_usage_scrubbing(self) -> None:
        """Test scrubbing a CostAndUsage dataset."""
        # Create test data
        df = pd.DataFrame(
            {
                "BillingAccountId": ["000011112222", "000011112222", "555566667777"],
                "BillingAccountName": ["Company A", "Company A", "Company B"],
                "SubAccountId": ["333344445555", "333344445555", "777788889999"],
                "SubAccountName": ["Team 1", "Team 1", "Team 2"],
                "CommitmentDiscountId": [
                    "arn:aws:ec2:us-east-1:333344445555:reserved-instances/00000000-1111-2222-3333-444444444444",
                    pd.NA,
                    "777788889999",
                ],
                "OtherColumn": ["unchanged1", "unchanged2", "unchanged3"],
            }
        )

        # Setup handlers
        config = HandlerConfig(date_shift_days=0)
        collector = MappingCollector()
        column_handlers, mapping_engine = get_column_handlers_for_dataset(
            "CostAndUsage", config=config, collector=collector
        )
        scrub = DataFrameScrub(column_handlers=column_handlers)

        # Scrub the data
        result = scrub.scrub(df)

        # Verify structure preserved
        assert len(result) == len(df)
        assert list(result.columns) == list(df.columns)

        # Verify account IDs changed
        assert result["BillingAccountId"][0] != df["BillingAccountId"][0]
        assert result["SubAccountId"][0] != df["SubAccountId"][0]

        # Verify consistency within column (same value maps to same result)
        assert result["BillingAccountId"][0] == result["BillingAccountId"][1]
        assert result["SubAccountId"][0] == result["SubAccountId"][1]

        # Verify names changed
        assert result["BillingAccountName"][0] != df["BillingAccountName"][0]
        assert result["SubAccountName"][0] != df["SubAccountName"][0]

        # Verify ARN was processed
        if pd.notna(result["CommitmentDiscountId"][0]):
            arn = str(result["CommitmentDiscountId"][0])
            assert arn.startswith("arn:aws:ec2:us-east-1:")
            # Account ID in ARN should NOT be the original
            assert "333344445555" not in arn

        # Verify unmapped column unchanged
        assert result["OtherColumn"][0] == df["OtherColumn"][0]

        # Verify mappings were collected
        mappings = collector.to_dict()
        assert "BillingAccountId" in mappings
        assert "SubAccountId" in mappings
        assert "BillingAccountName" in mappings

    def test_account_id_consistency_across_columns(self) -> None:
        """Test that same account ID maps consistently across different columns."""
        # Create test data where SubAccountId value appears in CommitmentDiscountId ARN
        account_id = "333344445555"
        df = pd.DataFrame(
            {
                "BillingAccountId": ["000011112222"],
                "SubAccountId": [account_id],
                "CommitmentDiscountId": [
                    f"arn:aws:ec2:us-east-1:{account_id}:reserved-instances/00000000-1111-2222-3333-444444444444"
                ],
            }
        )

        config = HandlerConfig(date_shift_days=0)
        column_handlers, mapping_engine = get_column_handlers_for_dataset(
            "CostAndUsage", config=config
        )
        scrub = DataFrameScrub(column_handlers=column_handlers)

        result = scrub.scrub(df)

        # Extract mapped account from SubAccountId
        mapped_account = result["SubAccountId"][0]

        # Extract account from ARN
        arn = result["CommitmentDiscountId"][0]
        arn_parts = arn.split(":")
        arn_account = arn_parts[4]

        # They should be the same
        assert mapped_account == arn_account
        assert mapped_account != account_id

    def test_load_and_reuse_mappings(self) -> None:
        """Test loading mappings and reusing them."""
        # First run: create mappings
        df1 = pd.DataFrame(
            {
                "BillingAccountId": ["111111111111", "888888888888"],
                "SubAccountId": ["222222222222", "999999999999"],
            }
        )

        config = HandlerConfig(date_shift_days=0)
        column_handlers1, mapping_engine1 = get_column_handlers_for_dataset(
            "CostAndUsage", config=config
        )
        scrub1 = DataFrameScrub(column_handlers=column_handlers1)
        result1 = scrub1.scrub(df1)

        # Export mappings (validated by get_all_mappings call)
        _ = mapping_engine1.get_all_mappings()

        # Second run: load mappings
        df2 = pd.DataFrame(
            {
                "BillingAccountId": ["111111111111"],  # Same as first run
                "SubAccountId": ["333333333333"],  # New value
            }
        )

        column_handlers2, mapping_engine2 = get_column_handlers_for_dataset(
            "CostAndUsage",
            config=config,
            mapping_engine=mapping_engine1,  # Reuse the engine
        )
        scrub2 = DataFrameScrub(column_handlers=column_handlers2)
        result2 = scrub2.scrub(df2)

        # Verify that the same account ID gets the same mapping
        assert result1["BillingAccountId"][0] == result2["BillingAccountId"][0]
