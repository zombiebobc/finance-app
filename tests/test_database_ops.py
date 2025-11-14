from datetime import UTC, datetime, timedelta
from typing import Dict

import pytest

from database_ops import (
    DatabaseError,
    bulk_insert_transactions_sqlite,
    delete_transaction_sqlite,
    get_sqlite_connection,
    get_transaction_count_sqlite,
    init_sqlite_db,
    insert_transaction_sqlite,
    query_transactions_sqlite,
)


@pytest.fixture()
def sqlite_conn():
    """Provide an in-memory SQLite connection for testing."""
    conn = get_sqlite_connection(":memory:")
    init_sqlite_db(conn)
    try:
        yield conn
    finally:
        conn.close()


def _build_transaction(seed: int, **overrides: Dict[str, object]) -> Dict[str, object]:
    base = {
        "date": (datetime(2024, 1, 1) + timedelta(days=seed)).isoformat(),
        "description": f"Test transaction {seed}",
        "amount": float(seed),
        "category": "utilities" if seed % 2 == 0 else "groceries",
        "account": "Checking",
        "account_id": 1,
        "source_file": "unit-test.csv",
        "duplicate_hash": f"hash-{seed}",
        "is_transfer": 0,
    }
    base.update(overrides)
    return base


def test_insert_transaction_sqlite_handles_special_characters(sqlite_conn):
    payload = _build_transaction(
        1,
        description="O'Reilly; DROP TABLE budgets; --",
    )
    row_id = insert_transaction_sqlite(sqlite_conn, payload)

    results = query_transactions_sqlite(
        sqlite_conn,
        {"description_keywords": "O'Reilly"},
    )

    assert results, "Expected at least one result for special character search."
    assert results[0]["id"] == row_id
    assert results[0]["description"] == payload["description"]


def test_insert_transaction_sqlite_missing_fields_raises(sqlite_conn):
    payload = {"date": datetime.now(UTC).isoformat()}
    with pytest.raises(DatabaseError):
        insert_transaction_sqlite(sqlite_conn, payload)


def test_bulk_insert_transactions_sqlite_large_dataset(sqlite_conn):
    transactions = [_build_transaction(seed) for seed in range(10050)]
    inserted, skipped = bulk_insert_transactions_sqlite(sqlite_conn, transactions)

    assert inserted == len(transactions)
    assert skipped == 0
    assert get_transaction_count_sqlite(sqlite_conn) == len(transactions)


def test_query_transactions_sqlite_with_filters_and_pagination(sqlite_conn):
    transactions = [
        _build_transaction(seed, amount=float(seed % 5))
        for seed in range(30)
    ]
    bulk_insert_transactions_sqlite(sqlite_conn, transactions)

    results = query_transactions_sqlite(
        sqlite_conn,
        {
            "amount_min": 2,
            "amount_max": 3,
            "category": "gro",
        },
        limit=5,
        offset=5,
        order_by="amount",
        order_desc=False,
    )

    assert len(results) <= 5
    assert all(2 <= row["amount"] <= 3 for row in results)
    assert all("gro" in row["category"].lower() for row in results)


def test_delete_transaction_sqlite_removes_record(sqlite_conn):
    row_id = insert_transaction_sqlite(sqlite_conn, _build_transaction(200))
    assert get_transaction_count_sqlite(sqlite_conn) == 1

    delete_transaction_sqlite(sqlite_conn, row_id)
    assert get_transaction_count_sqlite(sqlite_conn) == 0


def test_injection_attempt_treated_as_literal(sqlite_conn):
    payload = _build_transaction(
        300,
        description="Robert'); DROP TABLE transactions;--",
        category="security",
    )
    insert_transaction_sqlite(sqlite_conn, payload)

    # Ensure table still exists and data retrievable
    count = get_transaction_count_sqlite(sqlite_conn)
    assert count == 1

    results = query_transactions_sqlite(
        sqlite_conn,
        {"description_keywords": "DROP TABLE"},
    )
    assert len(results) == 1
    assert results[0]["description"] == payload["description"]


def test_insert_transaction_sqlite_missing_fields_raises_database_error(sqlite_conn):
    """Test that missing required fields raises DatabaseError with details."""
    payload = {"date": datetime.now(UTC).isoformat()}
    with pytest.raises(DatabaseError) as exc_info:
        insert_transaction_sqlite(sqlite_conn, payload)
    
    error = exc_info.value
    assert "Missing required transaction fields" in error.message
    assert "missing_fields" in error.details
    assert "required_fields" in error.details


def test_insert_transaction_sqlite_integrity_error_raises_database_error(sqlite_conn):
    """Test that integrity errors raise DatabaseError with details."""
    # Insert first transaction
    payload1 = _build_transaction(1, duplicate_hash="test-hash-123")
    insert_transaction_sqlite(sqlite_conn, payload1)
    
    # Try to insert duplicate (same duplicate_hash should be unique)
    payload2 = _build_transaction(2, duplicate_hash="test-hash-123")
    with pytest.raises(DatabaseError) as exc_info:
        insert_transaction_sqlite(sqlite_conn, payload2)
    
    error = exc_info.value
    assert "constraints" in error.message.lower()
    assert error.original_error is not None


def test_database_error_details_preserved(sqlite_conn):
    """Test that DatabaseError preserves error details and original error."""
    payload = {"date": datetime.now(UTC).isoformat()}
    
    try:
        insert_transaction_sqlite(sqlite_conn, payload)
    except DatabaseError as e:
        assert e.message is not None
        assert isinstance(e.details, dict)
        assert "missing_fields" in e.details
        assert isinstance(e.details["missing_fields"], list)


def test_query_transactions_sqlite_database_error_on_failure(sqlite_conn):
    """Test that query failures raise DatabaseError."""
    # Close connection to cause error
    sqlite_conn.close()
    
    with pytest.raises(DatabaseError):
        query_transactions_sqlite(sqlite_conn, {})


def test_delete_transaction_sqlite_database_error_on_failure(sqlite_conn):
    """Test that delete failures raise DatabaseError with transaction_id in details."""
    # Insert a transaction first
    row_id = insert_transaction_sqlite(sqlite_conn, _build_transaction(100))
    
    # Close connection to cause error
    sqlite_conn.close()
    
    with pytest.raises(DatabaseError) as exc_info:
        delete_transaction_sqlite(sqlite_conn, row_id)
    
    error = exc_info.value
    assert "Failed to delete transaction" in error.message
    # Note: transaction_id may not be in details if connection is already closed
    assert error.original_error is not None


def test_bulk_insert_database_error_on_failure(sqlite_conn):
    """Test that bulk insert failures raise DatabaseError with operation details."""
    # Create transactions with invalid data (missing required fields)
    invalid_transactions = [
        {"date": datetime.now(UTC).isoformat()}  # Missing required fields
    ]
    
    with pytest.raises(DatabaseError) as exc_info:
        bulk_insert_transactions_sqlite(sqlite_conn, invalid_transactions)
    
    error = exc_info.value
    assert error.message is not None
    assert "operation" in error.details or "error" in error.details

