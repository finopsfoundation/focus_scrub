from __future__ import annotations

from typing import Protocol

import pandas as pd


class ColumnHandler(Protocol):
    def scrub(self, value: object) -> object: ...


class DataFrameScrub:
    """Applies configured handlers per column."""

    def __init__(self, column_handlers: dict[str, ColumnHandler]):
        self._column_handlers = column_handlers

    def scrub(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        for column_name in result.columns:
            if column_name not in self._column_handlers:
                continue
            handler = self._column_handlers[column_name]
            result[column_name] = result[column_name].map(handler.scrub)
        return result
