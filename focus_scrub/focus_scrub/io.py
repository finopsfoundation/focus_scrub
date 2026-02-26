from __future__ import annotations

from enum import Enum
from pathlib import Path

import pandas as pd


class FileFormat(str, Enum):
    CSV_GZIP = "csv-gzip"
    PARQUET = "parquet"


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
    return output_root / f"{base}.parquet"


def write_focus_file(df: pd.DataFrame, output_file: Path, output_format: FileFormat) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_format == FileFormat.CSV_GZIP:
        df.to_csv(output_file, index=False, compression="gzip")
        return

    if output_format == FileFormat.PARQUET:
        df.to_parquet(output_file, index=False)
        return

    raise ValueError(f"Unsupported output format: {output_format}")


def _is_supported_input_file(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".csv") or name.endswith(".csv.gz") or name.endswith(".parquet")


def _strip_known_extensions(path: Path) -> str:
    text = str(path)
    for suffix in (".csv.gz", ".parquet", ".csv"):
        if text.lower().endswith(suffix):
            return text[: -len(suffix)]
    return text
