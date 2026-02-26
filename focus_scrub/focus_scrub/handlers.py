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
        # Handle scalar NA values
        try:
            if pd.isna(value):
                return value
        except (ValueError, TypeError):
            # Non-scalar or incompatible type, continue processing
            pass

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
        # Handle scalar NA values
        try:
            if pd.isna(value):
                return value
        except (ValueError, TypeError):
            # Non-scalar or incompatible type, continue processing
            pass

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
        # Handle scalar NA values
        try:
            if pd.isna(value):
                return value
        except (ValueError, TypeError):
            # Non-scalar or incompatible type, continue processing
            pass

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
        # Handle scalar NA values
        try:
            if pd.isna(value):
                return value
        except (ValueError, TypeError):
            # Non-scalar or incompatible type, continue processing
            pass

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
# Resource ID handler
# ---------------------------------------------------------------------------


@dataclass
class ResourceIdHandler:
    """Scrubs resource IDs with pattern matching and character-level scrambling."""

    mapping_engine: MappingEngine
    _char_map: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _column_name: str = field(default="", init=False, repr=False)
    _collector: MappingCollector | None = field(default=None, init=False, repr=False)

    # Regex patterns for specific resource types
    _ARN_PATTERN = re.compile(r"^arn:aws:[^:]+:[^:]*:(\d{12}):")
    _UUID_PATTERN = re.compile(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
    )
    _ACCOUNT_ID_PATTERN = re.compile(r"\d{12}")

    def attach_collector(self, column_name: str, collector: MappingCollector) -> None:
        self._column_name = column_name
        self._collector = collector

    def _get_char_mapping(self, char: str) -> str:
        """Get or create a random character mapping."""
        if char not in self._char_map:
            import random
            import string

            # Map to same character class
            if char.isupper():
                self._char_map[char] = random.choice(string.ascii_uppercase)
            elif char.islower():
                self._char_map[char] = random.choice(string.ascii_lowercase)
            elif char.isdigit():
                self._char_map[char] = random.choice(string.digits)
            else:
                # Non-alphanumeric characters stay the same
                self._char_map[char] = char

        return self._char_map[char]

    def _scramble_string(self, value: str) -> str:
        """Scramble a string by mapping each alphanumeric character."""
        return "".join(self._get_char_mapping(c) for c in value)

    def _scrub_arn(self, value: str) -> str:
        """Handle ARN format by scrubbing embedded account IDs, UUIDs, and resource names."""
        # ARN format: arn:partition:service:region:account-id:resource-type/resource-id
        # or: arn:partition:service:region:account-id:resource-id

        parts = value.split(":")
        if len(parts) < 6:
            # Not a valid ARN, just scramble the whole thing
            return self._scramble_string(value)

        # Parts: [arn, partition, service, region, account-id, resource...]
        # Keep the first 4 parts (arn:partition:service:region) unchanged
        prefix_parts = parts[:4]
        account_part = parts[4] if len(parts) > 4 else ""
        resource_parts = parts[5:] if len(parts) > 5 else []

        # Replace account ID if present
        if account_part and account_part.isdigit() and len(account_part) == 12:
            account_part = self.mapping_engine.map_number_id(account_part)

        # Join resource parts back (they were split by ':')
        resource_string = ":".join(resource_parts) if resource_parts else ""

        # Replace UUIDs in the resource string
        for match in self._UUID_PATTERN.finditer(resource_string):
            uuid_val = match.group(0)
            replacement = self.mapping_engine.map_uuid(uuid_val)
            resource_string = resource_string.replace(uuid_val, replacement)

        # Scramble resource names and IDs
        # The first segment before "/" is often resource-type (e.g., "loadbalancer", "log-group")
        # Subsequent segments are resource IDs/names that should be scrambled
        if resource_string:
            # Split by / for hierarchical resources
            slash_parts = resource_string.split("/")

            # First part might be "resource-type" or "resource-name"
            # Common AWS resource types to preserve
            aws_resource_types = {
                "loadbalancer",
                "log-group",
                "task",
                "cluster",
                "natgateway",
                "hostedzone",
                "table",
                "function",
                "distribution",
                "stream",
                "security-group",
                "network-interface",
                "volume",
                "snapshot",
            }

            scrambled_slash_parts = []
            for i, part in enumerate(slash_parts):
                # First segment: preserve if it's a known resource type, otherwise scramble
                if i == 0 and part in aws_resource_types:
                    scrambled_slash_parts.append(part)
                # "net" and "app" are load balancer scheme types, preserve them
                elif part in ["net", "app"]:
                    scrambled_slash_parts.append(part)
                else:
                    # Scramble everything else (resource names and IDs)
                    scrambled_slash_parts.append(self._scramble_string(part))

            resource_string = "/".join(scrambled_slash_parts)

        # Reconstruct the ARN
        result_parts = (
            prefix_parts + [account_part] + ([resource_string] if resource_string else [])
        )
        return ":".join(result_parts)

    def _scrub_azure_resource_id(self, value: str) -> str:
        """Handle Azure Resource ID format.

        Azure format: /subscriptions/{guid}/resourcegroups/{name}/providers/{namespace}/{type}/{name}
        or shorter: /subscriptions/{guid}/providers/{namespace}/{type}/{name}
        """
        # Split by / and process each segment
        parts = value.split("/")
        if len(parts) < 3:
            # Not a valid Azure Resource ID
            return self._scramble_string(value)

        scrubbed_parts = []
        i = 0
        while i < len(parts):
            part = parts[i]

            # Empty string at start (before first /)
            if not part:
                scrubbed_parts.append(part)
                i += 1
                continue

            # Preserve Azure structure keywords
            if part in ["subscriptions", "resourcegroups", "providers"]:
                scrubbed_parts.append(part)
                # Next part is the value - check if it's a subscription ID (UUID)
                if i + 1 < len(parts):
                    next_part = parts[i + 1]
                    if part == "subscriptions" and self._UUID_PATTERN.match(next_part):
                        # Scramble subscription ID (UUID)
                        scrubbed_parts.append(self.mapping_engine.map_uuid(next_part))
                        i += 2
                        continue
                    elif part in ["resourcegroups", "providers"]:
                        # Next part after resourcegroups/providers needs special handling
                        if part == "resourcegroups":
                            # Resource group name - scramble it
                            scrubbed_parts.append(self._scramble_string(next_part))
                            i += 2
                            continue
                        else:
                            # Provider namespace like "microsoft.compute" - preserve
                            scrubbed_parts.append(next_part)
                            i += 2
                            continue
                i += 1
            # Preserve Microsoft provider namespaces
            elif part.startswith("microsoft."):
                scrubbed_parts.append(part)
                i += 1
            # Check if it's a UUID embedded in a resource name
            elif self._UUID_PATTERN.search(part):
                # Replace UUIDs within the string
                scrubbed_part = part
                for match in self._UUID_PATTERN.finditer(part):
                    uuid_val = match.group(0)
                    replacement = self.mapping_engine.map_uuid(uuid_val)
                    scrubbed_part = scrubbed_part.replace(uuid_val, replacement)
                # Scramble the non-UUID parts
                # Split by UUID pattern and scramble text segments
                scrubbed_parts.append(scrubbed_part)
                i += 1
            else:
                # Resource type or resource name - scramble it
                scrubbed_parts.append(self._scramble_string(part))
                i += 1

        return "/".join(scrubbed_parts)

    def _scrub_oci_resource_id(self, value: str) -> str:
        """Handle OCI OCID format.

        OCI format: ocid1.{resource_type}.{realm}.{region}.{unique_id}
        or simple service names like: oci_{service_name}
        """
        # Handle simple OCI service names (e.g., oci_computeagent, oci_faas)
        if value.startswith("oci_") and "." not in value:
            # Just scramble the service name part after oci_
            parts = value.split("_", 1)
            if len(parts) == 2:
                return f"oci_{self._scramble_string(parts[1])}"
            return value

        # Handle full OCIDs: ocid1.{resource_type}.{realm}.{region}.{unique_id}
        if not value.startswith("ocid1."):
            return self._scramble_string(value)

        parts = value.split(".")
        if len(parts) < 5:
            # Not a valid OCID, scramble it
            return self._scramble_string(value)

        # Parts: [ocid1, resource_type, realm, region, unique_id]
        # Keep ocid1, realm (oc1), and region structure
        # Scramble resource_type and unique_id
        ocid_prefix = parts[0]  # "ocid1"
        resource_type = parts[1]  # e.g., "instance", "bootvolume", "vnic"
        realm = parts[2]  # e.g., "oc1"
        region = parts[3]  # e.g., "ap-hyderabad-1"
        unique_id = ".".join(parts[4:])  # The unique identifier (may contain dots)

        # Scramble resource type and unique ID
        scrambled_resource_type = self._scramble_string(resource_type)
        scrambled_unique_id = self._scramble_string(unique_id)

        return f"{ocid_prefix}.{scrambled_resource_type}.{realm}.{region}.{scrambled_unique_id}"

    def scrub(self, value: object) -> object:
        # Handle scalar NA values
        try:
            if pd.isna(value):
                return value
        except (ValueError, TypeError):
            # Non-scalar or incompatible type, continue processing
            pass

        original = str(value).strip()

        # Handle OCI OCIDs
        if original.startswith("ocid1.") or (original.startswith("oci_") and "." not in original):
            replacement = self._scrub_oci_resource_id(original)
        # Handle Azure Resource IDs
        elif original.startswith("/subscriptions/"):
            replacement = self._scrub_azure_resource_id(original)
        # Handle AWS ARNs
        elif original.startswith("arn:"):
            replacement = self._scrub_arn(original)
        # Handle instance IDs, ENI IDs, NAT gateway IDs, etc.
        elif original.startswith(("i-", "eni-", "nat-", "sg-", "subnet-", "vpc-", "vol-")):
            # Keep the prefix, scramble the rest
            prefix_end = original.find("-") + 1
            prefix = original[:prefix_end]
            suffix = original[prefix_end:]
            replacement = prefix + self._scramble_string(suffix)
        else:
            # Default: scramble the entire value
            replacement = self._scramble_string(original)

        if self._collector is not None:
            self._collector.record(self._column_name, original, replacement)

        return replacement


# ---------------------------------------------------------------------------
# Tags handler
# ---------------------------------------------------------------------------


@dataclass
class TagsHandler:
    """Scrubs Tags column by scrambling values while preserving keys.

    Handles different tag formats:
    - AWS: List of tuples [('key1', 'value1'), ('key2', 'value2')]
    - Azure/OCI: JSON string {"key1":"value1","key2":"value2"}
    """

    mapping_engine: MappingEngine
    _char_map: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _column_name: str = field(default="", init=False, repr=False)
    _collector: MappingCollector | None = field(default=None, init=False, repr=False)

    def attach_collector(self, column_name: str, collector: MappingCollector) -> None:
        self._column_name = column_name
        self._collector = collector

    def _get_char_mapping(self, char: str) -> str:
        """Get or create a random character mapping."""
        if char not in self._char_map:
            import random
            import string

            # Map to same character class
            if char.isupper():
                self._char_map[char] = random.choice(string.ascii_uppercase)
            elif char.islower():
                self._char_map[char] = random.choice(string.ascii_lowercase)
            elif char.isdigit():
                self._char_map[char] = random.choice(string.digits)
            else:
                # Non-alphanumeric characters stay the same
                self._char_map[char] = char

        return self._char_map[char]

    def _scramble_string(self, value: str) -> str:
        """Scramble a string by mapping each alphanumeric character."""
        return "".join(self._get_char_mapping(c) for c in value)

    def scrub(self, value: object) -> object:
        # Handle scalar NA values
        try:
            if pd.isna(value):
                return value
        except (ValueError, TypeError):
            # Non-scalar or incompatible type, continue processing
            pass

        # Handle actual list objects (AWS parquet format)
        if isinstance(value, list):
            if not value:  # Empty list
                return value
            # Scramble values but keep keys
            scrubbed_list = [(key, self._scramble_string(val)) for key, val in value]
            if self._collector is not None:
                self._collector.record(self._column_name, str(value), str(scrubbed_list))
            return scrubbed_list

        # Handle dict objects (less common but possible)
        if isinstance(value, dict):
            if not value:  # Empty dict
                return value
            scrubbed_dict = {
                key: self._scramble_string(val) if val else val for key, val in value.items()
            }
            if self._collector is not None:
                self._collector.record(self._column_name, str(value), str(scrubbed_dict))
            return scrubbed_dict

        original = str(value).strip()

        # Handle empty lists/dicts as strings
        if original in ("[]", "{}", ""):
            return value

        # Try to detect format and parse
        import ast
        import json

        try:
            # AWS format: list of tuples (comes in as string representation)
            if original.startswith("["):
                tags_list = ast.literal_eval(original)
                if isinstance(tags_list, list):
                    # Scramble values but keep keys
                    scrubbed_list = [(key, self._scramble_string(val)) for key, val in tags_list]
                    replacement = str(scrubbed_list)
                else:
                    replacement = original
            # Azure/OCI format: JSON string
            elif original.startswith("{"):
                tags_dict = json.loads(original)
                # Scramble values but keep keys
                scrubbed_dict = {
                    key: self._scramble_string(val) if val else val
                    for key, val in tags_dict.items()
                }
                replacement = json.dumps(scrubbed_dict, separators=(",", ":"))
            else:
                # Unknown format, pass through
                replacement = original
        except (ValueError, SyntaxError, json.JSONDecodeError):
            # If parsing fails, pass through unchanged
            replacement = original

        if self._collector is not None:
            self._collector.record(self._column_name, original, replacement)

        return replacement


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


def _build_resource_id_handler(config: HandlerConfig, engine: MappingEngine) -> ColumnHandler:
    return ResourceIdHandler(mapping_engine=engine)


def _build_tags_handler(config: HandlerConfig, engine: MappingEngine) -> ColumnHandler:
    return TagsHandler(mapping_engine=engine)


HANDLER_FACTORIES: dict[str, HandlerFactory] = {
    "DateReformat": _build_date_reformat_handler,
    "AccountId": _build_account_id_handler,
    "StellarName": _build_stellar_name_handler,
    "CommitmentDiscountId": _build_commitment_discount_id_handler,
    "ResourceId": _build_resource_id_handler,
    "Tags": _build_tags_handler,
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
        "ResourceId": "ResourceId",
        "Tags": "Tags",
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
        "Tags": "Tags",
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
