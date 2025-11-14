"""
Utility script to migrate legacy plaintext financial data to encrypted columns.

Usage:
    python encrypt_existing_data.py [--dry-run]

The script will:
    - Load the configured SQLite database.
    - Encrypt sensitive columns in-place using the shared EncryptionManager.
    - Populate the deterministic account name index used for uniqueness checks.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import yaml
from sqlalchemy.engine import make_url

from encryption_utils import (
    derive_search_token,
    encrypt_transaction_payload,
    get_encryption_manager,
    is_ciphertext,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("encrypt_migration")

CONFIG_PATH = Path("config.yaml")


def _load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _resolve_sqlite_path(config: Dict[str, Any]) -> Path:
    database_cfg = config.get("database", {})
    connection_string = database_cfg.get("connection_string") or f"sqlite:///{database_cfg.get('path', 'data/transactions.db')}"
    url = make_url(connection_string)
    if not url.database:
        raise ValueError("SQLite connection string missing database path.")
    db_path = Path(url.database)
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    return db_path


def _encrypt_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    python_type: type,
    *,
    dry_run: bool = False,
) -> int:
    """
    Encrypt a single column in-place. Returns number of rows updated.
    """
    manager = get_encryption_manager()
    try:
        cursor = conn.execute(f"SELECT id, {column} FROM {table}")
    except sqlite3.OperationalError:
        logger.info("Skipping %s.%s (table missing).", table, column)
        return 0
    updated = 0
    for row_id, current_value in cursor.fetchall():
        if current_value is None or (isinstance(current_value, str) and is_ciphertext(current_value)):
            continue
        if isinstance(current_value, bytes):
            plaintext = current_value.decode("utf-8")
        else:
            plaintext = current_value
        encrypted = manager.encrypt_value(plaintext, python_type)
        logger.debug("Encrypting %s.%s for row id=%s", table, column, row_id)
        if not dry_run:
            conn.execute(
                f"UPDATE {table} SET {column} = ? WHERE id = ?",
                (encrypted, row_id),
            )
        updated += 1
    return updated


def _normalize_timestamp(ts_value: Any) -> Optional[datetime]:
    """
    Convert legacy timezone-naive timestamps to UTC-aware datetime.
    
    Handles both string and datetime objects, ensuring all timestamps
    are stored as UTC-aware for consistency.
    """
    if ts_value is None:
        return None
    if isinstance(ts_value, datetime):
        if ts_value.tzinfo is None:
            # Assume naive datetime is UTC and make it timezone-aware
            return ts_value.replace(tzinfo=UTC)
        # Already timezone-aware, ensure it's UTC
        return ts_value.astimezone(UTC)
    if isinstance(ts_value, str):
        try:
            # Try parsing ISO format
            dt = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except (ValueError, AttributeError):
            logger.warning("Unable to parse timestamp: %s", ts_value)
            return None
    return None


def _encrypt_transactions(conn: sqlite3.Connection, dry_run: bool) -> int:
    """
    Encrypt transaction fields and normalize timestamps to UTC.
    
    Also handles timezone conversion for legacy timezone-naive timestamps.
    """
    try:
        cursor = conn.execute(
            """
            SELECT
                id,
                date,
                description,
                amount,
                category,
                account,
                account_id,
                source_file,
                import_timestamp,
                duplicate_hash,
                is_transfer,
                transfer_to_account_id
            FROM transactions
            """
        )
    except sqlite3.OperationalError:
        logger.info("Transactions table missing; skipping transactional encryption.")
        return 0
    manager = get_encryption_manager()
    updated = 0
    for row in cursor.fetchall():
        row_id = row[0]
        payload = {
            "date": row[1],
            "description": row[2],
            "amount": row[3],
            "category": row[4],
            "account": row[5],
            "account_id": row[6],
            "source_file": row[7],
            "import_timestamp": row[8],
            "duplicate_hash": row[9],
            "is_transfer": row[10],
            "transfer_to_account_id": row[11],
        }
        
        # Normalize timestamps to UTC-aware
        normalized_date = _normalize_timestamp(payload["date"])
        normalized_import_ts = _normalize_timestamp(payload["import_timestamp"])
        
        if all(isinstance(payload[field], str) and is_ciphertext(payload[field]) for field in ("description", "amount")):
            # Still update timestamps even if fields are already encrypted
            if (normalized_date and payload["date"] != normalized_date) or \
               (normalized_import_ts and payload["import_timestamp"] != normalized_import_ts):
                if not dry_run:
                    conn.execute(
                        """
                        UPDATE transactions
                        SET date = ?, import_timestamp = ?
                        WHERE id = ?
                        """,
                        (normalized_date, normalized_import_ts, row_id),
                    )
                updated += 1
            continue
        
        encrypted_payload = encrypt_transaction_payload(payload, skip_if_encrypted=True)
        if dry_run:
            updated += 1
            continue
        
        # Update with encrypted fields and normalized timestamps
        conn.execute(
            """
            UPDATE transactions
            SET description = ?, amount = ?, category = ?, account = ?, source_file = ?,
                date = ?, import_timestamp = ?
            WHERE id = ?
            """,
            (
                encrypted_payload.get("description"),
                encrypted_payload.get("amount"),
                encrypted_payload.get("category"),
                encrypted_payload.get("account"),
                encrypted_payload.get("source_file"),
                normalized_date or payload["date"],
                normalized_import_ts or payload["import_timestamp"],
                row_id,
            ),
        )
        updated += 1
    return updated


def _update_account_tokens(conn: sqlite3.Connection, dry_run: bool) -> int:
    manager = get_encryption_manager()
    cursor = conn.execute("SELECT id, name, balance, name_index FROM accounts")
    updated = 0
    for row_id, name_value, balance_value, name_index in cursor.fetchall():
        plaintext_name = None
        if name_value:
            if is_ciphertext(name_value):
                try:
                    plaintext_name = manager.decrypt_value(name_value, str)
                except Exception:
                    plaintext_name = None
            else:
                plaintext_name = str(name_value)
                encrypted_name = manager.encrypt_value(plaintext_name, str)
                if not dry_run:
                    conn.execute("UPDATE accounts SET name = ? WHERE id = ?", (encrypted_name, row_id))
                updated += 1
        if balance_value and not (isinstance(balance_value, str) and is_ciphertext(balance_value)):
            encrypted_balance = manager.encrypt_value(balance_value, float)
            if not dry_run:
                conn.execute("UPDATE accounts SET balance = ? WHERE id = ?", (encrypted_balance, row_id))
            updated += 1
        if plaintext_name and not name_index:
            token = derive_search_token(plaintext_name)
            if token and not dry_run:
                conn.execute("UPDATE accounts SET name_index = ? WHERE id = ?", (token, row_id))
            if token:
                updated += 1
    return updated


def run_migration(dry_run: bool = False) -> None:
    config = _load_config()
    db_path = _resolve_sqlite_path(config)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}")

    logger.info("Opening database at %s", db_path)
    conn = sqlite3.connect(db_path)
    try:
        changes = 0
        changes += _encrypt_transactions(conn, dry_run)
        changes += _encrypt_column(conn, "budgets", "category", str, dry_run=dry_run)
        changes += _encrypt_column(conn, "budgets", "allocated_amount", float, dry_run=dry_run)
        changes += _encrypt_column(conn, "income_overrides", "override_amount", float, dry_run=dry_run)
        changes += _encrypt_column(conn, "balance_history", "balance", float, dry_run=dry_run)
        changes += _encrypt_column(conn, "balance_overrides", "override_balance", float, dry_run=dry_run)
        changes += _update_account_tokens(conn, dry_run)
        if dry_run:
            logger.info("Dry run complete. %d rows would be updated.", changes)
            conn.rollback()
        else:
            conn.commit()
            logger.info("Migration complete. %d updates applied.", changes)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Encrypt legacy finance-app data in-place.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without modifying the database.",
    )
    args = parser.parse_args()
    run_migration(dry_run=args.dry_run)

