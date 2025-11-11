"""
Utility helpers for filesystem paths and configuration-driven resources.

Centralizes logic for resolving the project data directory and database
connection strings so both CLI tooling and Streamlit UIs stay in sync.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy.engine import make_url

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent
_DEFAULT_DATA_DIR_NAME = "data"
_DEFAULT_DB_FILENAME = "transactions.db"


def get_project_root() -> Path:
    """Return the repository root directory."""
    return _PROJECT_ROOT


def _coerce_path(path_value: str | Path, *, allow_relative: bool = True) -> Path:
    """
    Convert a string/Path into an absolute project-root based Path.
    
    Args:
        path_value: Candidate filesystem path.
        allow_relative: If False, value must already be absolute.
    
    Returns:
        Absolute Path instance.
    """
    path = Path(path_value)
    if path.is_absolute() or not allow_relative:
        return path
    return get_project_root() / path


def get_data_dir(config: Optional[Dict[str, Any]] = None) -> Path:
    """
    Resolve the data directory path without creating it.
    
    Args:
        config: Optional configuration dictionary.
    
    Returns:
        Path to the data directory (may not exist yet).
    """
    db_config = (config or {}).get("database", {})
    data_dir_raw = db_config.get("data_dir", _DEFAULT_DATA_DIR_NAME)
    return _coerce_path(data_dir_raw)


def ensure_data_dir(config: Optional[Dict[str, Any]] = None) -> Path:
    """
    Ensure the data directory exists and return its Path.
    
    Args:
        config: Optional configuration dictionary.
    
    Returns:
        Absolute Path to the ensured data directory.
    """
    data_dir = get_data_dir(config)
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.error("Failed to create data directory '%s': %s", data_dir, exc)
        raise
    return data_dir


def _ensure_sqlite_parent_dir(connection_string: str) -> None:
    """
    Ensure the parent directory for a SQLite database exists.
    
    Args:
        connection_string: SQLAlchemy connection string.
    """
    try:
        url = make_url(connection_string)
    except Exception as exc:  # pragma: no cover - defensive logging only
        logger.debug("Unable to parse connection string '%s': %s", connection_string, exc)
        return
    
    if not url.drivername.startswith("sqlite"):
        return
    
    database = url.database
    if not database:
        return
    
    db_path = Path(database)
    if not db_path.is_absolute():
        db_path = get_project_root() / db_path
    
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.error("Failed to create SQLite parent directory '%s': %s", db_path.parent, exc)
        raise


def resolve_connection_string(config: Optional[Dict[str, Any]] = None) -> str:
    """
    Resolve the database connection string using env var, config, or defaults.
    
    Order of precedence:
        1. DB_CONNECTION_STRING environment variable
        2. config['database']['connection_string']
        3. Constructed from data_dir/path defaults
    
    Args:
        config: Optional configuration dictionary.
    
    Returns:
        SQLAlchemy connection string.
    """
    config = config or {}
    env_conn = os.environ.get("DB_CONNECTION_STRING")
    if env_conn:
        if env_conn.strip() != "sqlite:///transactions.db":
            _ensure_sqlite_parent_dir(env_conn)
            return env_conn
    
    db_config = config.get("database", {})
    config_conn = db_config.get("connection_string")
    if config_conn:
        _ensure_sqlite_parent_dir(config_conn)
        return config_conn
    
    data_dir = ensure_data_dir(config)
    db_filename = db_config.get("path", _DEFAULT_DB_FILENAME)
    db_path = Path(db_filename)
    if not db_path.is_absolute():
        db_path = data_dir / db_path
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    
    connection_string = f"sqlite:///{db_path.as_posix()}"
    _ensure_sqlite_parent_dir(connection_string)
    return connection_string


def resolve_log_path(log_path: str) -> Path:
    """
    Convert a log file path to an absolute path under the project root when needed.
    
    Args:
        log_path: Configured log file path (relative or absolute).
    
    Returns:
        Absolute Path for logging output.
    """
    resolved = _coerce_path(log_path)
    if resolved.parent != resolved:
        resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved

