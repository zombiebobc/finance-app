"""
Tests for utility helpers used for data directory and connection resolution.
"""

from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

from utils import ensure_data_dir, resolve_connection_string


def test_ensure_data_dir_creates_directory(tmp_path):
    """ensure_data_dir should create the configured directory when missing."""
    config = {"database": {"data_dir": str(tmp_path / "finance_data")}}
    data_dir = ensure_data_dir(config)
    
    assert data_dir.exists()
    assert data_dir.is_dir()
    assert data_dir == Path(tmp_path / "finance_data")


def test_resolve_connection_string_default(monkeypatch, tmp_path):
    """resolve_connection_string should build a sqlite URL under the data dir."""
    monkeypatch.delenv("DB_CONNECTION_STRING", raising=False)
    data_dir = tmp_path / "app_data"
    config = {"database": {"data_dir": str(data_dir), "path": "budget.db"}}
    
    connection_string = resolve_connection_string(config)
    url = make_url(connection_string)
    
    assert url.drivername.startswith("sqlite")
    assert Path(url.database) == data_dir / "budget.db"
    assert (data_dir).exists()


def test_resolve_connection_string_env_override(monkeypatch, tmp_path):
    """Environment variable should take precedence over config/defaults."""
    db_path = tmp_path / "env_override" / "transactions.db"
    env_connection = f"sqlite:///{db_path.as_posix()}"
    monkeypatch.setenv("DB_CONNECTION_STRING", env_connection)
    
    try:
        connection_string = resolve_connection_string({})
        assert connection_string == env_connection
        assert db_path.parent.exists()
    finally:
        monkeypatch.delenv("DB_CONNECTION_STRING", raising=False)

