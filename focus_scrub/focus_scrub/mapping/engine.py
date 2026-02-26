"""Central mapping engine for consistent value transformations."""

from __future__ import annotations

import random
import string
import uuid
from collections.abc import Iterator


def _random_digits(length: int) -> str:
    """Return a random digit string of *length* with a non-zero leading digit."""
    if length <= 0:
        return ""
    first = random.choice(string.digits[1:])
    rest = "".join(random.choices(string.digits, k=length - 1))
    return first + rest


def _stellar_name_generator() -> Iterator[str]:
    """Generate unique stellar names by randomly combining stellar terms with Greek letters."""
    _STELLAR_TERMS = [
        "Moon",
        "Rocket",
        "Nebula",
        "Neuron",
        "Comet",
        "Galaxy",
        "Pulsar",
        "Quasar",
        "Supernova",
        "Meteor",
        "Asteroid",
        "Orbit",
        "Cosmos",
        "Photon",
        "Proton",
        "Quantum",
        "Plasma",
        "Eclipse",
        "Aurora",
        "Stellar",
        "Nova",
        "Vortex",
        "Zenith",
        "Helios",
        "Luna",
    ]

    _GREEK_LETTERS = [
        "Alpha",
        "Beta",
        "Gamma",
        "Delta",
        "Epsilon",
        "Zeta",
        "Eta",
        "Theta",
        "Iota",
        "Kappa",
        "Lambda",
        "Mu",
        "Nu",
        "Xi",
        "Omicron",
        "Pi",
        "Rho",
        "Sigma",
        "Tau",
        "Upsilon",
        "Phi",
        "Chi",
        "Psi",
        "Omega",
    ]

    # Create all possible combinations
    all_combinations = [(term, letter) for term in _STELLAR_TERMS for letter in _GREEK_LETTERS]
    # Shuffle to randomize order
    random.shuffle(all_combinations)

    # Yield all base combinations
    for term, letter in all_combinations:
        yield f"{term} {letter}"

    # If we exhaust combinations, add numeric suffixes with random selection
    count = 1
    while True:
        random.shuffle(all_combinations)
        for term, letter in all_combinations:
            yield f"{term} {letter} {count}"
        count += 1


class MappingEngine:
    """Central mapping engine that ensures consistent mappings across all columns."""

    def __init__(self) -> None:
        # Separate mapping dictionaries for each strategy
        self._number_id_map: dict[str, str] = {}
        self._uuid_map: dict[str, str] = {}
        self._name_map: dict[str, str] = {}
        self._profile_code_map: dict[str, str] = {}
        self._name_generator: Iterator[str] | None = None

    def map_number_id(self, value: str) -> str:
        """Map a numeric ID to another numeric ID of the same length."""
        if value in self._number_id_map:
            return self._number_id_map[value]

        replacement = _random_digits(len(value))
        self._number_id_map[value] = replacement
        return replacement

    def map_uuid(self, value: str) -> str:
        """Map a UUID to a new random UUID."""
        if value in self._uuid_map:
            return self._uuid_map[value]

        replacement = str(uuid.uuid4())
        self._uuid_map[value] = replacement
        return replacement

    def map_name(self, value: str) -> str:
        """Map a name to a stellar-themed name."""
        if value in self._name_map:
            return self._name_map[value]

        if self._name_generator is None:
            self._name_generator = _stellar_name_generator()

        replacement = next(self._name_generator)
        self._name_map[value] = replacement
        return replacement

    def map_profile_code(self, value: str) -> str:
        """Map a profile code to a random profile code of the same structure."""
        if value in self._profile_code_map:
            return self._profile_code_map[value]

        segments = value.split("-")
        random_segments = [
            "".join(random.choices(string.ascii_uppercase + string.digits, k=len(seg)))
            for seg in segments
        ]
        replacement = "-".join(random_segments)
        self._profile_code_map[value] = replacement
        return replacement

    def get_all_mappings(self) -> dict[str, dict[str, str]]:
        """Return all mappings grouped by strategy."""
        return {
            "NumberId": dict(self._number_id_map),
            "UUID": dict(self._uuid_map),
            "Name": dict(self._name_map),
            "ProfileCode": dict(self._profile_code_map),
        }

    def load_mappings(self, mappings: dict[str, dict[str, str]]) -> None:
        """Load mappings from a dictionary (e.g., from a previously exported file).

        This allows reusing the same mappings across multiple executions.
        """
        if "NumberId" in mappings:
            self._number_id_map.update(mappings["NumberId"])
        if "UUID" in mappings:
            self._uuid_map.update(mappings["UUID"])
        if "Name" in mappings:
            self._name_map.update(mappings["Name"])
        if "ProfileCode" in mappings:
            self._profile_code_map.update(mappings["ProfileCode"])
