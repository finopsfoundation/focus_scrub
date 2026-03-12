"""Tests for file I/O functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
from focus_scrub.io import FileFormat, write_focus_file


class TestSqlOutput:
    """Test SQL output format."""

    def test_sql_output_basic(self) -> None:
        """Test basic SQL INSERT statement generation."""
        df = pd.DataFrame(
            {
                "BillingAccountId": ["123456789012", "987654321098"],
                "BillingAccountName": ["Account A", "Account B"],
                "Cost": [100.50, 250.75],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_output.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            # Read the generated SQL
            sql_content = output_file.read_text()

            # Verify header comments
            assert "-- FOCUS Scrubbed Data" in sql_content
            assert "-- Table: test_output" in sql_content
            assert "-- Rows: 2" in sql_content

            # Verify INSERT statement structure
            assert "INSERT INTO test_output" in sql_content
            assert "(BillingAccountId, BillingAccountName, Cost)" in sql_content
            assert "VALUES" in sql_content

            # Verify data values
            assert "'123456789012'" in sql_content
            assert "'Account A'" in sql_content
            assert "100.5" in sql_content
            assert "'987654321098'" in sql_content
            assert "'Account B'" in sql_content
            assert "250.75" in sql_content

    def test_sql_output_with_nulls(self) -> None:
        """Test SQL output handles NULL values correctly."""
        df = pd.DataFrame(
            {
                "AccountId": ["123456789012", "987654321098"],
                "Description": ["Valid", None],
                "Amount": [100.0, pd.NA],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_nulls.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Verify NULL handling
            assert "NULL" in sql_content
            # First row should have valid value
            assert "'Valid'" in sql_content
            # Second row should have NULL for Description and Amount
            assert "('987654321098', NULL, NULL)" in sql_content

    def test_sql_output_escapes_quotes(self) -> None:
        """Test SQL output properly escapes single quotes."""
        df = pd.DataFrame(
            {
                "AccountId": ["123456789012"],
                "Description": ["Account with 'quotes' in name"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_escaping.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Verify quotes are escaped ('' in SQL)
            assert "Account with ''quotes'' in name" in sql_content

    def test_sql_output_empty_dataframe(self) -> None:
        """Test SQL output handles empty DataFrame."""
        df = pd.DataFrame({"Col1": [], "Col2": []})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_empty.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Should have header
            assert "-- FOCUS Scrubbed Data" in sql_content
            assert "-- Rows: 0" in sql_content
            # Should indicate no data
            assert "-- No data to insert" in sql_content

    def test_sql_output_large_batch(self) -> None:
        """Test SQL output splits large datasets into batches."""
        # Create a dataset with 2500 rows (should create 3 batches of 1000, 1000, 500)
        df = pd.DataFrame(
            {
                "Id": [f"id_{i}" for i in range(2500)],
                "Value": list(range(2500)),
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_batches.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Should have 3 INSERT statements
            insert_count = sql_content.count("INSERT INTO")
            assert insert_count == 3, f"Expected 3 INSERT statements, got {insert_count}"

            # Verify all rows are present
            for i in [0, 1000, 2000, 2499]:  # Check first, batch boundaries, and last
                assert f"'id_{i}'" in sql_content

    def test_sql_output_table_name_sanitization(self) -> None:
        """Test SQL output sanitizes table names."""
        df = pd.DataFrame({"Col1": [1]})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test-with-hyphens.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Hyphens should be replaced with underscores
            assert "INSERT INTO test_with_hyphens" in sql_content
            assert "-- Table: test_with_hyphens" in sql_content

    def test_sql_output_table_name_removes_periods(self) -> None:
        """Test SQL output removes periods from table names."""
        df = pd.DataFrame({"Col1": [1]})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test.with.periods.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Periods should be replaced with underscores
            assert "CREATE TABLE IF NOT EXISTS test_with_periods" in sql_content
            assert "INSERT INTO test_with_periods" in sql_content
            assert "-- Table: test_with_periods" in sql_content

    def test_sql_output_with_complex_types(self) -> None:
        """Test SQL output handles complex types (lists, dicts)."""
        df = pd.DataFrame(
            {
                "AccountId": ["123456789012"],
                "Tags": ['{"environment": "prod", "team": "alpha"}'],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_complex.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Complex types should be converted to strings
            assert "environment" in sql_content
            assert "prod" in sql_content
            assert "alpha" in sql_content

    def test_sql_output_with_array_values(self) -> None:
        """Test SQL output handles array-like values without ValueError."""
        import numpy as np

        # Create a DataFrame with array values (similar to what pandas might pass)
        df = pd.DataFrame(
            {
                "Col1": ["value1", "value2"],
                "Col2": [np.array([1, 2, 3]), np.array([4, 5, 6])],
                "Col3": [100, 200],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_array.sql"
            # This should not raise ValueError from pd.isna()
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Verify SQL was generated
            assert "INSERT INTO test_array" in sql_content
            assert "value1" in sql_content
            assert "value2" in sql_content
            # Array should be converted to string
            assert "[1 2 3]" in sql_content or "[1, 2, 3]" in sql_content

    def test_sql_output_includes_create_table(self) -> None:
        """Test SQL output includes CREATE TABLE statement."""
        df = pd.DataFrame(
            {
                "AccountId": ["123", "456"],
                "Cost": [100.50, 250.75],
                "Quantity": [10, 20],
                "BillingDate": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test_schema.sql"
            write_focus_file(df, output_file, FileFormat.SQL)

            sql_content = output_file.read_text()

            # Verify CREATE TABLE statement
            assert "CREATE TABLE IF NOT EXISTS test_schema" in sql_content

            # Verify id column is first with AUTO_INCREMENT PRIMARY KEY
            assert "id BIGINT AUTO_INCREMENT PRIMARY KEY" in sql_content

            # Verify column definitions with types
            assert "AccountId TEXT" in sql_content
            assert "Cost DOUBLE PRECISION" in sql_content
            assert "Quantity BIGINT" in sql_content
            assert "BillingDate TIMESTAMP" in sql_content

            # Verify CREATE TABLE comes before INSERT
            create_pos = sql_content.find("CREATE TABLE")
            insert_pos = sql_content.find("INSERT INTO")
            assert create_pos < insert_pos
