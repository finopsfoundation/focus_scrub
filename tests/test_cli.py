"""Tests for CLI functionality."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest


class TestCLI:
    """Test CLI integration."""

    def test_cli_basic_run(self) -> None:
        """Test basic CLI execution."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            # Create a simple test file
            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "BillingAccountName": ["TestAccount"],
                    "BillingPeriodStart": ["2024-01-01T00:00:00Z"],
                    "BillingPeriodEnd": ["2024-01-31T23:59:59Z"],
                }
            )
            df.to_csv(test_file, index=False)

            # Run CLI
            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
            ]

            result = main()
            assert result == 0

            # Check output was created
            output_files = list(output_dir.rglob("*.parquet"))
            assert len(output_files) == 1

    def test_cli_with_csv_gzip_output(self) -> None:
        """Test CLI with CSV gzip output format."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            test_file = input_dir / "test.parquet"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "BillingAccountName": ["TestAccount"],
                    "BillingPeriodStart": ["2024-01-01T00:00:00Z"],
                    "BillingPeriodEnd": ["2024-01-31T23:59:59Z"],
                }
            )
            df.to_parquet(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--output-format",
                "csv-gzip",
            ]

            result = main()
            assert result == 0

            output_files = list(output_dir.rglob("*.csv.gz"))
            assert len(output_files) == 1

    def test_cli_with_date_shift(self) -> None:
        """Test CLI with date shifting."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "BillingPeriodStart": ["2024-01-01T00:00:00Z"],
                    "BillingPeriodEnd": ["2024-01-31T23:59:59Z"],
                }
            )
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--date-shift-days",
                "30",
            ]

            result = main()
            assert result == 0

    def test_cli_with_mappings_export(self) -> None:
        """Test CLI with mappings export."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            mappings_file = Path(tmpdir) / "mappings.json"
            input_dir.mkdir()

            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012", "987654321098"],
                    "BillingAccountName": ["TestAccount", "OtherAccount"],
                }
            )
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--export-mappings",
                str(mappings_file),
            ]

            result = main()
            assert result == 0

            # Check mappings file was created
            assert mappings_file.exists()
            mappings_data = json.loads(mappings_file.read_text())
            assert "column_mappings" in mappings_data
            assert "component_mappings" in mappings_data

    def test_cli_with_mappings_load(self) -> None:
        """Test CLI with loading mappings."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            mappings_file = Path(tmpdir) / "mappings.json"
            input_dir.mkdir()

            # Create mappings file
            mappings_data = {
                "column_mappings": {},
                "component_mappings": {
                    "number_id": {"123456789012": "999999999999"},
                    "name": {"TestAccount": "MappedAccount"},
                },
            }
            mappings_file.write_text(json.dumps(mappings_data))

            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "BillingAccountName": ["TestAccount"],
                }
            )
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--load-mappings",
                str(mappings_file),
            ]

            result = main()
            assert result == 0

    def test_cli_with_remove_custom_columns(self) -> None:
        """Test CLI with remove custom columns option."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "x_CustomColumn": ["value"],
                    "oci_CustomColumn": ["value"],
                }
            )
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--remove-custom-columns",
            ]

            result = main()
            assert result == 0

    def test_cli_with_drop_columns(self) -> None:
        """Test CLI with drop columns option."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "x_Discounts": ["value"],
                    "OtherColumn": ["value"],
                }
            )
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--drop-columns",
                "OtherColumn",
            ]

            result = main()
            assert result == 0

    def test_cli_with_scrub_tag_keys(self) -> None:
        """Test CLI with scrub tag keys option."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "Tags": ['{"key": "value"}'],
                }
            )
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--scrub-tag-keys",
            ]

            result = main()
            assert result == 0

    def test_cli_with_dates_only(self) -> None:
        """Test CLI with dates only option."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "BillingPeriodStart": ["2024-01-01T00:00:00Z"],
                }
            )
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--dates-only",
            ]

            result = main()
            assert result == 0

    def test_cli_with_sql_output_and_table_name(self) -> None:
        """Test CLI with SQL output and custom table name."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            test_file = input_dir / "test.csv"
            df = pd.DataFrame(
                {
                    "BillingAccountId": ["123456789012"],
                    "Cost": [100.50],
                }
            )
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--output-format",
                "sql",
                "--sql-table-name",
                "my_custom_table",
            ]

            result = main()
            assert result == 0

            output_files = list(output_dir.rglob("*.sql"))
            assert len(output_files) == 1

            sql_content = output_files[0].read_text()
            assert "my_custom_table" in sql_content

    def test_cli_no_input_files(self) -> None:
        """Test CLI with no input files."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
            ]

            with pytest.raises(SystemExit):
                main()

    def test_cli_missing_mappings_file(self) -> None:
        """Test CLI with missing mappings file."""
        from focus_scrub.cli import main

        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            mappings_file = Path(tmpdir) / "missing_mappings.json"
            input_dir.mkdir()

            test_file = input_dir / "test.csv"
            df = pd.DataFrame({"BillingAccountId": ["123456789012"]})
            df.to_csv(test_file, index=False)

            import sys

            sys.argv = [
                "focus-scrub",
                str(input_dir),
                str(output_dir),
                "--dataset",
                "CostAndUsage",
                "--load-mappings",
                str(mappings_file),
            ]

            with pytest.raises(SystemExit):
                main()
