from __future__ import annotations

from typing import Protocol

import pandas as pd


class ColumnHandler(Protocol):
    def scrub(self, value: object) -> object: ...


class DataFrameScrub:
    """Applies configured handlers per column."""

    def __init__(
        self, column_handlers: dict[str, ColumnHandler], remove_custom_columns: bool = False
    ):
        self._column_handlers = column_handlers
        self._remove_custom_columns = remove_custom_columns

    def _is_custom_column(self, column_name: str) -> bool:
        """Check if a column is a custom column (x_* or oci_*)."""
        return column_name.startswith("x_") or column_name.startswith("oci_")

    def scrub(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        # Remove custom columns if requested
        if self._remove_custom_columns:
            custom_columns = [col for col in result.columns if self._is_custom_column(col)]
            if custom_columns:
                result = result.drop(columns=custom_columns)

        # Apply handlers to remaining columns
        for column_name in result.columns:
            if column_name not in self._column_handlers:
                continue
            handler = self._column_handlers[column_name]
            result[column_name] = result[column_name].map(handler.scrub)
        return result
