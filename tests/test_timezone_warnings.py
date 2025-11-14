"""
Tests to verify no deprecation warnings are emitted.

This test suite ensures that:
- No datetime.utcnow() deprecation warnings
- No SQLAlchemy cache_ok warnings
- All timestamps are timezone-aware
"""

import pytest
import warnings
from datetime import UTC, datetime
from sqlalchemy import create_engine, Column, Integer, DateTime
from sqlalchemy.orm import declarative_base

from encryption_utils import EncryptedString, EncryptedNumeric
from database_ops import utc_now, Base, Account


def test_utc_now_is_timezone_aware():
    """Verify utc_now() returns timezone-aware datetime."""
    now = utc_now()
    assert now.tzinfo is not None
    assert now.tzinfo == UTC


def test_no_datetime_utcnow_warnings():
    """Verify no datetime.utcnow() deprecation warnings are emitted."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        # Trigger code that uses utc_now()
        now = utc_now()
        # Check for any utcnow-related warnings
        utcnow_warnings = [
            warning for warning in w
            if "utcnow" in str(warning.message).lower() or "utcnow" in str(warning.filename)
        ]
        assert len(utcnow_warnings) == 0, f"Found utcnow deprecation warnings: {utcnow_warnings}"


def test_encrypted_types_have_cache_ok():
    """Verify encrypted types have cache_ok set to True."""
    # Check class attribute
    assert EncryptedString.cache_ok is True
    assert EncryptedNumeric.cache_ok is True
    
    # Check instance attribute
    encrypted_str = EncryptedString()
    encrypted_num = EncryptedNumeric()
    assert encrypted_str.cache_ok is True
    assert encrypted_num.cache_ok is True


def test_no_cache_ok_warnings():
    """Verify no SQLAlchemy cache_ok warnings are emitted when using encrypted types."""
    # Create a simple model with encrypted columns
    TestBase = declarative_base()
    
    class TestModel(TestBase):
        __tablename__ = "test_cache_model"
        id = Column(Integer, primary_key=True)
        encrypted_field = Column(EncryptedString(255))
        encrypted_number = Column(EncryptedNumeric())
        timestamp = Column(DateTime(timezone=True), default=utc_now)
    
    # Create in-memory database
    engine = create_engine("sqlite:///:memory:")
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        TestBase.metadata.create_all(engine)
        
        # Check for cache_ok warnings
        cache_warnings = [
            warning for warning in w
            if "cache_ok" in str(warning.message).lower()
        ]
        assert len(cache_warnings) == 0, f"Found cache_ok warnings: {cache_warnings}"


def test_account_model_timestamps_are_timezone_aware():
    """Verify Account model uses timezone-aware DateTime columns."""
    # Check that Account model columns are timezone-aware
    account_table = Account.__table__
    created_at_col = account_table.c.created_at
    updated_at_col = account_table.c.updated_at
    
    assert created_at_col.type.timezone is True
    assert updated_at_col.type.timezone is True


def test_transaction_model_timestamps_are_timezone_aware():
    """Verify Transaction model uses timezone-aware DateTime columns."""
    from database_ops import Transaction
    
    transaction_table = Transaction.__table__
    date_col = transaction_table.c.date
    import_timestamp_col = transaction_table.c.import_timestamp
    
    assert date_col.type.timezone is True
    assert import_timestamp_col.type.timezone is True

