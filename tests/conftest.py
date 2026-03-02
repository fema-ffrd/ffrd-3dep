"""Test configuration."""

import os
import sys


def pytest_configure(config):
    """Configure pytest with custom markers and ensure src is on sys.path."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may download data)"
    )
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_dir = os.path.join(repo_root, "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
