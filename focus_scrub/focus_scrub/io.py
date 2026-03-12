from __future__ import annotations

from enum import Enum
from pathlib import Path

import pandas as pd


class FileFormat(str, Enum):
    CSV_GZIP = "csv-gzip"
    PARQUET = "parquet"
    SQL = "sql"


SUPPORTED_INPUT_EXTENSIONS: tuple[str, ...] = (".csv", ".csv.gz", ".parquet")


def discover_focus_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        if _is_supported_input_file(input_path):
            return [input_path]
        return []

    files: list[Path] = []
    for candidate in sorted(input_path.rglob("*")):
        if candidate.is_file() and _is_supported_input_file(candidate):
            files.append(candidate)
    return files


def read_focus_file(path: Path) -> pd.DataFrame:
    suffixes = path.suffixes

    if path.suffix == ".parquet":
        return pd.read_parquet(path)

    if suffixes[-2:] == [".csv", ".gz"] or path.suffix == ".csv":
        return pd.read_csv(path)

    raise ValueError(f"Unsupported input format: {path}")


def output_path_for_file(
    input_file: Path,
    input_root: Path,
    output_root: Path,
    output_format: FileFormat,
) -> Path:
    relative = input_file.relative_to(input_root) if input_root.is_dir() else input_file.name

    if isinstance(relative, str):
        relative_path = Path(relative)
    else:
        relative_path = relative

    base = _strip_known_extensions(relative_path)

    if output_format == FileFormat.CSV_GZIP:
        return output_root / f"{base}.csv.gz"
    if output_format == FileFormat.SQL:
        return output_root / f"{base}.sql"
    return output_root / f"{base}.parquet"


def write_focus_file(
    df: pd.DataFrame,
    output_file: Path,
    output_format: FileFormat,
    sql_table_name: str | None = None,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_format == FileFormat.CSV_GZIP:
        df.to_csv(output_file, index=False, compression="gzip")
        return

    if output_format == FileFormat.PARQUET:
        df.to_parquet(output_file, index=False)
        return

    if output_format == FileFormat.SQL:
        _write_sql_insert_statements(df, output_file, sql_table_name)
        return

    raise ValueError(f"Unsupported output format: {output_format}")


def _is_supported_input_file(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".csv") or name.endswith(".csv.gz") or name.endswith(".parquet")


def _strip_known_extensions(path: Path) -> str:
    text = str(path)
    for suffix in (".csv.gz", ".parquet", ".csv", ".sql"):
        if text.lower().endswith(suffix):
            return text[: -len(suffix)]
    return text


def _pandas_dtype_to_sql_type(dtype: str) -> str:
    """Map pandas dtype to SQL type."""
    dtype_str = str(dtype).lower()

    if "int" in dtype_str:
        return "BIGINT"
    elif "float" in dtype_str or "double" in dtype_str:
        return "DOUBLE PRECISION"
    elif "bool" in dtype_str:
        return "BOOLEAN"
    elif "datetime" in dtype_str or "timestamp" in dtype_str:
        return "TIMESTAMP"
    elif "date" in dtype_str:
        return "DATE"
    else:
        # Default to TEXT for strings and other types
        return "TEXT"


def _write_sql_insert_statements(
    df: pd.DataFrame, output_file: Path, sql_table_name: str | None = None
) -> None:
    """Write DataFrame as SQL CREATE TABLE and INSERT statements.

    Generates CREATE TABLE DDL and bulk INSERT statements with proper SQL escaping.
    Table name is derived from the output filename unless sql_table_name is provided.
    """
    # Use custom table name if provided, otherwise derive from filename
    if sql_table_name:
        table_name = sql_table_name
    else:
        table_name = output_file.stem
    # Sanitize table name (replace hyphens/spaces/periods with underscores)
    table_name = table_name.replace("-", "_").replace(" ", "_").replace(".", "_")

    with open(output_file, "w") as f:
        # Write header comment
        f.write("-- FOCUS Scrubbed Data\n")
        f.write(f"-- Table: {table_name}\n")
        f.write(f"-- Rows: {len(df)}\n\n")

        # Get column names and types
        columns = df.columns.tolist()

        # Write CREATE TABLE statement
        f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (\n")
        column_defs = ["  id BIGINT AUTO_INCREMENT PRIMARY KEY"]
        for col in columns:
            sql_type = _pandas_dtype_to_sql_type(df[col].dtype)
            column_defs.append(f"  {col} {sql_type}")
        f.write(",\n".join(column_defs))
        f.write("\n);\n\n")

        if len(df) == 0:
            f.write("-- No data to insert\n")
            return

        # Column list for INSERT statements
        column_list = ", ".join(f"{col}" for col in columns)

        # Write bulk INSERT in batches of 1000 rows
        batch_size = 1000
        for batch_start in range(0, len(df), batch_size):
            batch_end = min(batch_start + batch_size, len(df))
            batch_df = df.iloc[batch_start:batch_end]

            f.write(f"INSERT INTO {table_name} ({column_list})\n")
            f.write("VALUES\n")

            for idx, (_, row) in enumerate(batch_df.iterrows()):
                values = []
                for col in columns:
                    value = row[col]
                    # Handle different data types
                    try:
                        is_na = pd.isna(value)
                        # If pd.isna returns an array, treat as not NA
                        if hasattr(is_na, "__len__") and not isinstance(is_na, str):
                            is_na = False
                    except (ValueError, TypeError):
                        # For array-like values, pd.isna() may fail
                        is_na = False

                    if is_na:
                        values.append("NULL")
                    elif isinstance(value, (int, float)):
                        # Check if it's actually NaN
                        try:
                            is_na_num = pd.isna(value)
                            if hasattr(is_na_num, "__len__") and not isinstance(is_na_num, str):
                                is_na_num = False
                        except (ValueError, TypeError):
                            is_na_num = False

                        if is_na_num:
                            values.append("NULL")
                        else:
                            values.append(str(value))
                    elif isinstance(value, str):
                        # Escape single quotes
                        escaped = value.replace("'", "''")
                        values.append(f"'{escaped}'")
                    else:
                        # For other types (e.g., lists, dicts), convert to string
                        escaped = str(value).replace("'", "''")
                        values.append(f"'{escaped}'")

                value_list = ", ".join(values)
                # Add comma if not the last row in batch
                if idx < len(batch_df) - 1:
                    f.write(f"  ({value_list}),\n")
                else:
                    f.write(f"  ({value_list});\n\n")
