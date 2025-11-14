"""
Backup and restore utilities for the finance-app database.

Provides functions to create timestamped backups of the database file
and restore from those backups with safety checks and user confirmation.
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from sqlalchemy.engine import make_url

from exceptions import FinanceAppError, DatabaseError

logger = logging.getLogger(__name__)


class BackupError(FinanceAppError):
    """Raised when backup or restore operations fail."""
    pass


def extract_db_path_from_connection_string(connection_string: str) -> Path:
    """
    Extract the database file path from a SQLAlchemy connection string.
    
    Args:
        connection_string: SQLAlchemy connection string (e.g., 'sqlite:///data/transactions.db')
    
    Returns:
        Absolute Path to the database file
    
    Raises:
        BackupError: If connection string cannot be parsed or is not SQLite
    """
    try:
        url = make_url(connection_string)
    except Exception as exc:
        raise BackupError(
            f"Invalid connection string: {connection_string}",
            details={"connection_string": connection_string},
            original_error=exc
        ) from exc
    
    if not url.drivername.startswith("sqlite"):
        raise BackupError(
            f"Backup only supports SQLite databases, got: {url.drivername}",
            details={"driver": url.drivername}
        )
    
    database = url.database
    if not database:
        raise BackupError(
            "Connection string does not specify a database path",
            details={"connection_string": connection_string}
        )
    
    db_path = Path(database)
    if not db_path.is_absolute():
        # Resolve relative to project root
        # Get project root (parent of utils directory)
        project_root = Path(__file__).resolve().parent.parent
        db_path = project_root / db_path
    
    return db_path.resolve()


def get_backup_dir(db_path: Optional[Path] = None, config: Optional[dict] = None) -> Path:
    """
    Get the backup directory path.
    
    Defaults to data/backups/ relative to the database file's parent directory.
    Can be overridden via config['backup']['backup_dir'].
    
    Args:
        db_path: Optional database file path (used to infer backup location)
        config: Optional configuration dictionary
    
    Returns:
        Absolute Path to the backup directory
    """
    # Check config for custom backup directory
    if config:
        backup_config = config.get("backup", {})
        backup_dir_raw = backup_config.get("backup_dir")
        if backup_dir_raw:
            backup_path = Path(backup_dir_raw)
            if not backup_path.is_absolute():
                project_root = Path(__file__).resolve().parent.parent
                backup_path = project_root / backup_path
            return backup_path.resolve()
    
    # Default: use data/backups/ relative to database parent
    if db_path:
        backup_dir = db_path.parent / "backups"
    else:
        # Fallback to project root / data / backups
        project_root = Path(__file__).resolve().parent.parent
        data_dir = project_root / "data"
        backup_dir = data_dir / "backups"
    
    return backup_dir.resolve()


def create_backup(db_path: str, config: Optional[dict] = None) -> str:
    """
    Create a timestamped backup of the database file.
    
    Args:
        db_path: Database file path (can be connection string or file path)
        config: Optional configuration dictionary
    
    Returns:
        Path to the created backup file
    
    Raises:
        BackupError: If backup creation fails (missing DB, permission issues, etc.)
    """
    # Handle connection string or file path
    if isinstance(db_path, str) and db_path.startswith("sqlite:///"):
        db_file_path = extract_db_path_from_connection_string(db_path)
    else:
        db_file_path = Path(db_path).resolve()
    
    # Check if database file exists
    if not db_file_path.exists():
        raise BackupError(
            f"Database file not found: {db_file_path}",
            details={"db_path": str(db_file_path)}
        )
    
    # Get backup directory
    backup_dir = get_backup_dir(db_file_path, config)
    
    # Create backup directory if it doesn't exist
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError) as exc:
        raise BackupError(
            f"Failed to create backup directory: {backup_dir}",
            details={"backup_dir": str(backup_dir)},
            original_error=exc
        ) from exc
    
    # Generate timestamped backup filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"transactions_backup_{timestamp}.db"
    backup_path = backup_dir / backup_filename
    
    # Copy database file to backup location
    try:
        logger.info(f"Creating backup: {backup_path}")
        shutil.copy2(db_file_path, backup_path)
        logger.info(f"Backup created successfully: {backup_path}")
    except (OSError, PermissionError, shutil.Error) as exc:
        raise BackupError(
            f"Failed to create backup: {exc}",
            details={
                "source": str(db_file_path),
                "destination": str(backup_path)
            },
            original_error=exc
        ) from exc
    
    # Verify backup was created and matches original size
    if not backup_path.exists():
        raise BackupError(
            "Backup file was not created",
            details={"backup_path": str(backup_path)}
        )
    
    original_size = db_file_path.stat().st_size
    backup_size = backup_path.stat().st_size
    if original_size != backup_size:
        # Clean up partial backup
        try:
            backup_path.unlink()
        except OSError:
            pass
        raise BackupError(
            f"Backup size mismatch: original={original_size}, backup={backup_size}",
            details={
                "original_size": original_size,
                "backup_size": backup_size,
                "backup_path": str(backup_path)
            }
        )
    
    return str(backup_path)


def list_backups(backup_dir: Optional[str] = None, db_path: Optional[str] = None, config: Optional[dict] = None) -> List[str]:
    """
    List all available backup files in the backup directory.
    
    Args:
        backup_dir: Optional explicit backup directory path
        db_path: Optional database path (used to infer backup location)
        config: Optional configuration dictionary
    
    Returns:
        List of backup file paths, sorted by modification time (newest first)
    
    Raises:
        BackupError: If backup directory cannot be accessed
    """
    if backup_dir:
        backup_dir_path = Path(backup_dir).resolve()
    else:
        # Infer from db_path or config
        if db_path:
            if isinstance(db_path, str) and db_path.startswith("sqlite:///"):
                db_file_path = extract_db_path_from_connection_string(db_path)
            else:
                db_file_path = Path(db_path).resolve()
        else:
            db_file_path = None
        backup_dir_path = get_backup_dir(db_file_path, config)
    
    # Check if backup directory exists
    if not backup_dir_path.exists():
        logger.warning(f"Backup directory does not exist: {backup_dir_path}")
        return []
    
    # Find all .db files in backup directory
    try:
        backup_files = [
            str(f) for f in backup_dir_path.glob("transactions_backup_*.db")
            if f.is_file()
        ]
    except (OSError, PermissionError) as exc:
        raise BackupError(
            f"Failed to list backups in directory: {backup_dir_path}",
            details={"backup_dir": str(backup_dir_path)},
            original_error=exc
        ) from exc
    
    # Sort by modification time (newest first)
    backup_files.sort(key=lambda f: Path(f).stat().st_mtime, reverse=True)
    
    return backup_files


def restore_backup(backup_path: str, db_path: str, force: bool = False) -> None:
    """
    Restore a database from a backup file.
    
    Args:
        backup_path: Path to the backup file to restore from
        db_path: Target database path (can be connection string or file path)
        force: If True, skip confirmation prompt (use with caution)
    
    Raises:
        BackupError: If restore fails (missing backup, permission issues, etc.)
    """
    # Resolve backup path
    backup_file = Path(backup_path).resolve()
    if not backup_file.exists():
        raise BackupError(
            f"Backup file not found: {backup_path}",
            details={"backup_path": str(backup_file)}
        )
    
    # Resolve target database path
    if isinstance(db_path, str) and db_path.startswith("sqlite:///"):
        db_file_path = extract_db_path_from_connection_string(db_path)
    else:
        db_file_path = Path(db_path).resolve()
    
    # Warn about overwriting existing database
    if db_file_path.exists() and not force:
        import sys
        print(
            f"\n⚠️  WARNING: This will overwrite the existing database: {db_file_path}",
            file=sys.stderr
        )
        print(
            "⚠️  Make sure the application is closed before restoring.",
            file=sys.stderr
        )
        response = input("Continue? (yes/no): ").strip().lower()
        if response not in ("yes", "y"):
            print("Restore cancelled.", file=sys.stderr)
            return
    
    # Ensure target directory exists
    try:
        db_file_path.parent.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError) as exc:
        raise BackupError(
            f"Failed to create target directory: {db_file_path.parent}",
            details={"target_dir": str(db_file_path.parent)},
            original_error=exc
        ) from exc
    
    # Copy backup to target location
    try:
        logger.info(f"Restoring backup: {backup_file} -> {db_file_path}")
        shutil.copy2(backup_file, db_file_path)
        logger.info(f"Restore completed successfully: {db_file_path}")
    except (OSError, PermissionError, shutil.Error) as exc:
        raise BackupError(
            f"Failed to restore backup: {exc}",
            details={
                "backup_path": str(backup_file),
                "target_path": str(db_file_path)
            },
            original_error=exc
        ) from exc
    
    # Verify restore was successful
    if not db_file_path.exists():
        raise BackupError(
            "Restored database file was not created",
            details={"target_path": str(db_file_path)}
        )
    
    backup_size = backup_file.stat().st_size
    restored_size = db_file_path.stat().st_size
    if backup_size != restored_size:
        raise BackupError(
            f"Restore size mismatch: backup={backup_size}, restored={restored_size}",
            details={
                "backup_size": backup_size,
                "restored_size": restored_size,
                "target_path": str(db_file_path)
            }
        )

