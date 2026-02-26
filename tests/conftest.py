"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import pytest

from focus_scrub.mapping import MappingCollector, MappingEngine


@pytest.fixture
def mapping_engine() -> MappingEngine:
    """Provide a fresh MappingEngine instance for each test."""
    return MappingEngine()


@pytest.fixture
def mapping_collector() -> MappingCollector:
    """Provide a fresh MappingCollector instance for each test."""
    return MappingCollector()
