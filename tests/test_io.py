"""Tests for file I/O functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from focus_scrub.io import (
    FileFormat,
    discover_focus_files,
    output_path_for_file,
    read_focus_file,
    write_focus_file,
)


class TestFileDiscovery:
    """Test file discovery functions."""

    def test_discover_single_csv_file(self) -> None:
        """Test discovering a single CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.csv"
            test_file.write_text("col1,col2\n1,2")

            files = discover_focus_files(test_file)
            assert len(files) == 1
            assert files[0] == test_file

    def test_discover_single_csv_gz_file(self) -> None:
        """Test discovering a single CSV.GZ file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.csv.gz"
            df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
            df.to_csv(test_file, index=False, compression="gzip")

            files = discover_focus_files(test_file)
            assert len(files) == 1
            assert files[0] == test_file

    def test_discover_single_parquet_file(self) -> None:
        """Test discovering a single parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.parquet"
            df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
            df.to_parquet(test_file, index=False)

            files = discover_focus_files(test_file)
            assert len(files) == 1
            assert files[0] == test_file

    def test_discover_unsupported_file(self) -> None:
        """Test discovering an unsupported file returns empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("not a supported file")

            files = discover_focus_files(test_file)
            assert len(files) == 0

    def test_discover_directory_with_multiple_files(self) -> None:
        """Test discovering multiple files in a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dir_path = Path(tmpdir)

            # Create multiple supported files
            csv_file = dir_path / "test1.csv"
            csv_file.write_text("col1\n1")

            parquet_file = dir_path / "test2.parquet"
            df = pd.DataFrame({"col1": [1]})
            df.to_parquet(parquet_file, index=False)

            # Create unsupported file
            txt_file = dir_path / "test.txt"
            txt_file.write_text("ignore")

            files = discover_focus_files(dir_path)
            assert len(files) == 2
            assert csv_file in files
            assert parquet_file in files
            assert txt_file not in files

    def test_discover_nested_directories(self) -> None:
        """Test discovering files in nested directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dir_path = Path(tmpdir)
            nested_dir = dir_path / "subdir"
            nested_dir.mkdir()

            file1 = dir_path / "test1.csv"
            file1.write_text("col1\n1")

            file2 = nested_dir / "test2.csv"
            file2.write_text("col1\n2")

            files = discover_focus_files(dir_path)
            assert len(files) == 2
            assert file1 in files
            assert file2 in files


class TestReadFocusFile:
    """Test reading FOCUS files."""

    def test_read_csv_file(self) -> None:
        """Test reading a CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.csv"
            df_original = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
            df_original.to_csv(test_file, index=False)

            df = read_focus_file(test_file)
            assert df.shape == (2, 2)
            assert list(df.columns) == ["col1", "col2"]

    def test_read_csv_gz_file(self) -> None:
        """Test reading a CSV.GZ file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.csv.gz"
            df_original = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
            df_original.to_csv(test_file, index=False, compression="gzip")

            df = read_focus_file(test_file)
            assert df.shape == (2, 2)
            assert list(df.columns) == ["col1", "col2"]

    def test_read_parquet_file(self) -> None:
        """Test reading a parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.parquet"
            df_original = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
            df_original.to_parquet(test_file, index=False)

            df = read_focus_file(test_file)
            assert df.shape == (2, 2)
            assert list(df.columns) == ["col1", "col2"]

    def test_read_unsupported_file_raises_error(self) -> None:
        """Test reading an unsupported file raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("not supported")

            with pytest.raises(ValueError, match="Unsupported input format"):
                read_focus_file(test_file)


class TestOutputPathForFile:
    """Test output path generation."""

    def test_output_path_for_file_parquet_default(self) -> None:
        """Test generating output path with default parquet format."""
        input_file = Path("/input/dir/file.csv")
        input_root = Path("/input/dir")
        output_root = Path("/output/dir")

        output = output_path_for_file(input_file, input_root, output_root, FileFormat.PARQUET)
        assert output == Path("/output/dir/file.parquet")

    def test_output_path_for_file_csv_gzip(self) -> None:
        """Test generating output path with CSV gzip format."""
        input_file = Path("/input/dir/file.parquet")
        input_root = Path("/input/dir")
        output_root = Path("/output/dir")

        output = output_path_for_file(input_file, input_root, output_root, FileFormat.CSV_GZIP)
        assert output == Path("/output/dir/file.csv.gz")

    def test_output_path_for_file_sql(self) -> None:
        """Test generating output path with SQL format."""
        input_file = Path("/input/dir/file.csv")
        input_root = Path("/input/dir")
        output_root = Path("/output/dir")

        output = output_path_for_file(input_file, input_root, output_root, FileFormat.SQL)
        assert output == Path("/output/dir/file.sql")

    def test_output_path_preserves_subdirectory(self) -> None:
        """Test output path preserves subdirectory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            input_dir.mkdir()
            subdir = input_dir / "subdir"
            subdir.mkdir()

            input_file = subdir / "file.csv"
            input_file.write_text("col1\n1")

            output_root = Path(tmpdir) / "output"

            output = output_path_for_file(input_file, input_dir, output_root, FileFormat.PARQUET)
            assert output == output_root / "subdir" / "file.parquet"

    def test_output_path_with_file_as_input_root(self) -> None:
        """Test output path when input_root is a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "file.csv"
            input_file.write_text("col1\n1")
            output_root = Path(tmpdir) / "output"

            # When input_root is a file (not a dir), it uses just the name
            output = output_path_for_file(input_file, input_file, output_root, FileFormat.PARQUET)
            assert output == output_root / "file.parquet"


class TestWriteFocusFile:
    """Test writing FOCUS files."""

    def test_write_csv_gzip(self) -> None:
        """Test writing a CSV.GZ file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.csv.gz"
            df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

            write_focus_file(df, output_file, FileFormat.CSV_GZIP)

            assert output_file.exists()
            df_read = pd.read_csv(output_file, compression="gzip")
            assert df_read.shape == (2, 2)

    def test_write_parquet(self) -> None:
        """Test writing a parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

            write_focus_file(df, output_file, FileFormat.PARQUET)

            assert output_file.exists()
            df_read = pd.read_parquet(output_file)
            assert df_read.shape == (2, 2)

    def test_write_creates_parent_directory(self) -> None:
        """Test that write_focus_file creates parent directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "subdir" / "output.parquet"
            df = pd.DataFrame({"col1": [1]})

            write_focus_file(df, output_file, FileFormat.PARQUET)

            assert output_file.exists()
            assert output_file.parent.exists()


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

    def test_sql_output_custom_table_name(self) -> None:
        """Test SQL output with custom table name."""
        df = pd.DataFrame(
            {
                "AccountId": ["123", "456"],
                "Cost": [100.50, 250.75],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "some-file.name.sql"
            write_focus_file(df, output_file, FileFormat.SQL, sql_table_name="custom_focus_table")

            sql_content = output_file.read_text()

            # Verify custom table name is used
            assert "CREATE TABLE IF NOT EXISTS custom_focus_table" in sql_content
            assert "INSERT INTO custom_focus_table" in sql_content
            assert "-- Table: custom_focus_table" in sql_content

            # Verify filename-based name is NOT used
            assert "some_file_name" not in sql_content
