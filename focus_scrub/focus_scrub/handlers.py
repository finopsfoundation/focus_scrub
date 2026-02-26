from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

import pandas as pd

from focus_scrub.mapping import MappingCollector, MappingEngine


class ColumnHandler(Protocol):
    def scrub(self, value: object) -> object: ...

    def attach_collector(self, column_name: str, collector: MappingCollector) -> None: ...


@dataclass
class GeneratorMappingHandler:
    generator_factory: Callable[[], Iterator[str]]
    value_map: dict[str, str] = field(default_factory=dict)
    _generator: Iterator[str] | None = field(default=None, init=False, repr=False)
    _column_name: str = field(default="", init=False, repr=False)
    _collector: MappingCollector | None = field(default=None, init=False, repr=False)

    def attach_collector(self, column_name: str, collector: MappingCollector) -> None:
        self._column_name = column_name
        self._collector = collector

    def scrub(self, value: object) -> object:
        if pd.isna(value):
            return value

        normalized = str(value)
        if normalized not in self.value_map:
            if self._generator is None:
                self._generator = self.generator_factory()
            self.value_map[normalized] = next(self._generator)
            if self._collector is not None:
                self._collector.record(self._column_name, normalized, self.value_map[normalized])

        return self.value_map[normalized]


@dataclass
class DateReformatHandler:
    days_to_add: int = 0
    _column_name: str = field(default="", init=False, repr=False)
    _collector: MappingCollector | None = field(default=None, init=False, repr=False)

    def attach_collector(self, column_name: str, collector: MappingCollector) -> None:
        self._column_name = column_name
        self._collector = collector

    def scrub(self, value: object) -> object:
        if pd.isna(value):
            return value

        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return value

        shifted = parsed + pd.Timedelta(days=self.days_to_add)

        if isinstance(value, str):
            result: object = shifted.isoformat()
        elif isinstance(value, datetime):
            result = shifted.to_pydatetime()
        else:
            result = shifted

        if self._collector is not None and self.days_to_add != 0:
            self._collector.record(self._column_name, str(value), str(result))

        return result


# ---------------------------------------------------------------------------
# Account ID handler
# ---------------------------------------------------------------------------

# Matches a standard 8-4-4-4-12 UUID anywhere in a string.
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)

# Matches dash-separated alphanumeric codes like JW5R-JYGR-BG7-PGB
# (typically 4 segments of 3-4 chars each, uppercase alphanumeric)
_PROFILE_CODE_RE = re.compile(
    r"\b[A-Z0-9]{3,4}(?:-[A-Z0-9]{3,4}){2,}\b",
)

# Matches 12-digit account IDs
_12DIGIT_ACCOUNT_ID_RE = re.compile(r"\b\d{12}\b")


@dataclass
class AccountIdHandler:
    """Scrubs account IDs using a shared mapping engine for consistency."""

    mapping_engine: MappingEngine
    _column_name: str = field(default="", init=False, repr=False)
    _collector: MappingCollector | None = field(default=None, init=False, repr=False)

    def attach_collector(self, column_name: str, collector: MappingCollector) -> None:
        self._column_name = column_name
        self._collector = collector

    def scrub(self, value: object) -> object:
        if pd.isna(value):
            return value

        original = str(value).strip()
        replacement = self._scrub_value(original)

        if self._collector is not None:
            self._collector.record(self._column_name, original, replacement)

        return replacement

    def _scrub_value(self, value: str) -> str:
        """Scrub a value by delegating to the mapping engine."""
        # Pure numeric: use NumberId mapping
        if value.isdigit():
            return self.mapping_engine.map_number_id(value)

        # Contains UUIDs, profile codes, or account IDs: map each component consistently
        if (
            _UUID_RE.search(value)
            or _PROFILE_CODE_RE.search(value)
            or _12DIGIT_ACCOUNT_ID_RE.search(value)
        ):
            result = value

            # Map all 12-digit account IDs
            def replace_account_id(match: re.Match[str]) -> str:
                account_id = match.group(0)
                return self.mapping_engine.map_number_id(account_id)

            result = _12DIGIT_ACCOUNT_ID_RE.sub(replace_account_id, result)

            # Map all UUIDs
            def replace_uuid(match: re.Match[str]) -> str:
                uuid_str = match.group(0)
                return self.mapping_engine.map_uuid(uuid_str)

            result = _UUID_RE.sub(replace_uuid, result)

            # Map all profile codes
            def replace_profile_code(match: re.Match[str]) -> str:
                code = match.group(0)
                return self.mapping_engine.map_profile_code(code)

            result = _PROFILE_CODE_RE.sub(replace_profile_code, result)

            return result

        # Fallback: opaque string → UUID
        return self.mapping_engine.map_uuid(value)


# ---------------------------------------------------------------------------
# Stellar name handler
# ---------------------------------------------------------------------------


@dataclass
class StellarNameHandler:
    """Maps arbitrary account names to stellar-themed generated names."""

    mapping_engine: MappingEngine
    _column_name: str = field(default="", init=False, repr=False)
    _collector: MappingCollector | None = field(default=None, init=False, repr=False)

    def attach_collector(self, column_name: str, collector: MappingCollector) -> None:
        self._column_name = column_name
        self._collector = collector

    def scrub(self, value: object) -> object:
        if pd.isna(value):
            return value

        original = str(value).strip()
        replacement = self.mapping_engine.map_name(original)

        if self._collector is not None:
            self._collector.record(self._column_name, original, replacement)

        return replacement


# ---------------------------------------------------------------------------
# Commitment Discount ID handler
# ---------------------------------------------------------------------------


@dataclass
class CommitmentDiscountIdHandler:
    """Scrubs commitment discount IDs by delegating to AccountIdHandler."""

    mapping_engine: MappingEngine
    _account_id_handler: AccountIdHandler = field(init=False, repr=False)
    _column_name: str = field(default="", init=False, repr=False)
    _collector: MappingCollector | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        # Create an AccountIdHandler with the shared mapping engine
        self._account_id_handler = AccountIdHandler(mapping_engine=self.mapping_engine)

    def attach_collector(self, column_name: str, collector: MappingCollector) -> None:
        self._column_name = column_name
        self._collector = collector
        # Pass through to the underlying handler with the same column name
        self._account_id_handler.attach_collector(column_name, collector)

    def scrub(self, value: object) -> object:
        # Delegate to AccountIdHandler
        return self._account_id_handler.scrub(value)


# ---------------------------------------------------------------------------
# Handler config + factories
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HandlerConfig:
    date_shift_days: int = 0


HandlerFactory = Callable[[HandlerConfig, MappingEngine], ColumnHandler]


def _build_date_reformat_handler(config: HandlerConfig, engine: MappingEngine) -> ColumnHandler:
    return DateReformatHandler(days_to_add=config.date_shift_days)


def _build_account_id_handler(config: HandlerConfig, engine: MappingEngine) -> ColumnHandler:
    return AccountIdHandler(mapping_engine=engine)


def _build_stellar_name_handler(config: HandlerConfig, engine: MappingEngine) -> ColumnHandler:
    return StellarNameHandler(mapping_engine=engine)


def _build_commitment_discount_id_handler(
    config: HandlerConfig, engine: MappingEngine
) -> ColumnHandler:
    return CommitmentDiscountIdHandler(mapping_engine=engine)


HANDLER_FACTORIES: dict[str, HandlerFactory] = {
    "DateReformat": _build_date_reformat_handler,
    "AccountId": _build_account_id_handler,
    "StellarName": _build_stellar_name_handler,
    "CommitmentDiscountId": _build_commitment_discount_id_handler,
}

# Dataset-specific column mapping.
#
# Map dataset name -> (column name -> handler name).
DATASET_COLUMN_HANDLER_NAMES: dict[str, dict[str, str]] = {
    "CostAndUsage": {
        "BillingPeriodStart": "DateReformat",
        "BillingPeriodEnd": "DateReformat",
        "ChargePeriodStart": "DateReformat",
        "ChargePeriodEnd": "DateReformat",
        "BillingAccountId": "AccountId",
        "BillingAccountName": "StellarName",
        "SubAccountId": "AccountId",
        "SubAccountName": "StellarName",
        "CommitmentDiscountId": "CommitmentDiscountId",
    },
    "ContractCommitment": {
        "ContractCommitmentPeriodStart": "DateReformat",
        "ContractCommitmentPeriodEnd": "DateReformat",
        "ContractPeriodStart": "DateReformat",
        "ContractPeriodEnd": "DateReformat",
        "BillingAccountId": "AccountId",
        "BillingAccountName": "StellarName",
        "SubAccountId": "AccountId",
        "SubAccountName": "StellarName",
        "CommitmentDiscountId": "CommitmentDiscountId",
    },
}


def list_datasets() -> list[str]:
    return sorted(DATASET_COLUMN_HANDLER_NAMES.keys())


def get_column_handlers_for_dataset(
    dataset_name: str,
    *,
    config: HandlerConfig,
    collector: MappingCollector | None = None,
    mapping_engine: MappingEngine | None = None,
) -> tuple[dict[str, ColumnHandler], MappingEngine]:
    if dataset_name not in DATASET_COLUMN_HANDLER_NAMES:
        supported = ", ".join(list_datasets())
        raise ValueError(f"Unknown dataset '{dataset_name}'. Supported datasets: {supported}")

    column_to_handler_name = DATASET_COLUMN_HANDLER_NAMES[dataset_name]
    column_handlers: dict[str, ColumnHandler] = {}

    # Create a shared mapping engine for all handlers to ensure consistent mappings
    # Or use the provided one if loading from existing mappings
    if mapping_engine is None:
        mapping_engine = MappingEngine()

    for column_name, handler_name in column_to_handler_name.items():
        if handler_name not in HANDLER_FACTORIES:
            raise ValueError(
                f"Dataset '{dataset_name}' references unknown handler "
                f"'{handler_name}' for column '{column_name}'."
            )

        # Create handler using the factory with the shared mapping engine
        handler = HANDLER_FACTORIES[handler_name](config, mapping_engine)

        if collector is not None:
            handler.attach_collector(column_name, collector)
        column_handlers[column_name] = handler

    return column_handlers, mapping_engine
