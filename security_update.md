## Security Update Summary

### Core Improvements
- Replaced raw string formatting with parameterized `sqlite3` execution helpers to neutralize SQL injection inputs.
- Wrapped every database call in `try/except sqlite3.Error` blocks that log context and re-raise a `DatabaseError` with a user-friendly message.
- Added schema bootstrap helpers that create defensive indexes (`date`, `account_id, date`, `duplicate_hash`) to keep lookups fast as data volume grows.
- Introduced reusable connection factory with safe defaults (row factory, `check_same_thread=False`) for easier dependency injection and concurrent usage.
- Expanded automated coverage with `pytest` cases that assert behavior for special-character payloads, injection attempts, empty result sets, and 10k+ row inserts.

### Before/After Example (Key Function)

```diff
- cursor.execute(
-     f"INSERT INTO transactions (date, description, amount) VALUES ('{date}', '{desc}', {amount})"
- )
+ cursor.execute(
+     "INSERT INTO transactions (date, description, amount) VALUES (?, ?, ?)",  # Fixed: Parameterized query
+     (date, desc, amount),
+ )
```

### Rationale
- **SQL Injection Mitigation:** Parameter bindings ensure that malicious payloads such as `Robert'); DROP TABLE transactions;--` are handled strictly as data, preserving table integrity.
- **Operational Resilience:** Centralized `DatabaseError` abstraction keeps the public API clean while preserving diagnostic detail in logs for developers.
- **Performance Safeguards:** Lightweight indexes on critical columns (`date`, `account_id`, `duplicate_hash`) reduce query latency for reporting and duplicate-detection workflows.
- **Testability:** Dependency-injected connections and `sqlite3.Row` configuration allow deterministic, in-memory test runs that mirror production semantics.
- **Scalability:** Bulk insert helpers rely on `executemany` with bindings, enabling consistent throughput even when seeding 10k+ transactions.

### Next Steps
- Evaluate adopting SQLCipher or another SQLite extension for optional file-level encryption (`toggle_encryption` hook can be layered on top of the new helpers).
- Consider migrating complex query logic to an ORM or query builder (e.g., SQLAlchemy core or SQLModel) while retaining the hardened parameterization patterns.

---

### 2025-11-14 — Field-Level Encryption Rollout
- Added `encryption_utils.py` providing Fernet key management, SQLAlchemy `TypeDecorator`s, and SQLite UDFs (`decrypt_text`, `decrypt_numeric`) so sensitive fields stay encrypted at rest yet remain queryable.
- Hardened ORM models (`Account`, `Transaction`, `Budget`, etc.) by wrapping string/numeric columns with encrypted types and automatic deterministic name indexes for uniqueness.
- Updated `database_ops.py` helpers (SQLite inserts/queries) to encrypt payloads before writes and decrypt via SQL functions during reads and filtering.
- Introduced `encrypt_existing_data.py` migration utility plus config/README guidance to generate or load keys from environment variables or `config.yaml`.
- Expanded automated coverage with `tests/test_encryption_utils.py` to validate key handling, round-trip encryption, and deterministic search tokens.

### 2025-01-XX — Deprecation Warning Fixes and Timezone Hardening
- **SQLAlchemy Cache-Key Warnings**: Added explicit `cache_ok = True` annotations to `EncryptedString`, `EncryptedNumeric`, and base `EncryptedType` classes to resolve SQLAlchemy 2.x cache-key conflict warnings. This ensures encrypted types are properly cacheable in query compilation. The attribute is set both as a class attribute and in `__init__` to ensure SQLAlchemy recognizes it at instance level.
- **SQLAlchemy declarative_base() Deprecation**: Updated import to use `sqlalchemy.orm.declarative_base()` instead of deprecated `sqlalchemy.ext.declarative.declarative_base()` to eliminate MovedIn20Warning.
- **Datetime Deprecation Fixes**: Replaced all `datetime.utcnow()` calls with timezone-aware `datetime.now(UTC)` throughout the codebase (`database_ops.py`, `account_management.py`, `budgeting.py`, `tests/test_database_ops.py`). All DateTime columns in SQLAlchemy models now use `DateTime(timezone=True)` to ensure timezone-aware storage.
- **Timezone Configuration**: Added optional `security.timezone` configuration in `config.yaml` (defaults to UTC). All timestamps are stored in UTC in the database, with timezone conversion handled at the application layer if needed.
- **Migration Script Updates**: Updated `encrypt_existing_data.py` to handle timezone-aware datetime conversions during data migration, ensuring legacy data without timezones is properly converted to UTC via the `_normalize_timestamp()` helper function.
- **Test Coverage**: Added `tests/test_timezone_warnings.py` with comprehensive tests for `cache_ok` attribute verification, timezone-aware datetime handling, and warning-free execution to ensure no regressions.

