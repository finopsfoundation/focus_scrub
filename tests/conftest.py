"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import pytest

from focus_scrub.mapping import MappingEngine


@pytest.fixture
def mapping_engine() -> MappingEngine:
    """Provide a fresh MappingEngine instance for each test."""
    return MappingEngine()
