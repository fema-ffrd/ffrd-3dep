"""Test configuration."""

import os
import sys


def pytest_configure():
    """Ensure the src directory is on sys.path for imports."""
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_dir = os.path.join(repo_root, "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
