import importlib
from datetime import UTC, datetime

import pytest
from cryptography.fernet import Fernet

import encryption_utils


@pytest.fixture(autouse=True)
def _reset_manager(monkeypatch):
    """
    Reset the encryption manager singleton with a predictable test key.
    """
    key = Fernet.generate_key().decode("utf-8")
    monkeypatch.setenv("FINANCE_APP_ENCRYPTION_KEY", key)
    importlib.reload(encryption_utils)
    yield
    importlib.reload(encryption_utils)


def test_encrypt_decrypt_round_trip_text():
    manager = encryption_utils.get_encryption_manager()
    token = manager.encrypt_value("secret description", str)
    assert token != "secret description"

    decrypted = manager.decrypt_value(token, str)
    assert decrypted == "secret description"


def test_encrypt_decrypt_round_trip_numeric():
    manager = encryption_utils.get_encryption_manager()
    token = manager.encrypt_value(42.13, float)
    assert token

    decrypted = manager.decrypt_value(token, float)
    assert pytest.approx(decrypted, rel=1e-9) == 42.13


def test_derive_search_token_is_deterministic():
    token_a1 = encryption_utils.derive_search_token("Checking")
    token_a2 = encryption_utils.derive_search_token("checking ")
    token_b = encryption_utils.derive_search_token("Savings")

    assert token_a1 == token_a2
    assert token_a1 != token_b


def test_encrypt_transaction_payload_marks_sensitive_fields():
    payload = {
        "description": "Coffee shop run",
        "amount": 12.34,
        "category": "Food",
        "account": "Checking",
        "source_file": "test.csv",
    }
    encrypted = encryption_utils.encrypt_transaction_payload(payload)

    assert encrypted["description"] != payload["description"]
    assert encrypted["amount"] != payload["amount"]
    assert encrypted["category"] != payload["category"]


def test_cache_ok_attribute_on_encrypted_types():
    """Test that EncryptedString and EncryptedNumeric have cache_ok=True for SQLAlchemy 2.x."""
    from encryption_utils import EncryptedString, EncryptedNumeric, EncryptedType
    
    # Base class should have cache_ok
    assert EncryptedType.cache_ok is True
    
    # Derived classes should explicitly set it
    assert EncryptedString.cache_ok is True
    assert EncryptedNumeric.cache_ok is True


def test_timezone_aware_datetime_handling():
    """Test that timezone-aware datetimes work correctly with encryption."""
    from database_ops import utc_now, Transaction
    from datetime import UTC
    
    # Verify utc_now() returns timezone-aware datetime
    now = utc_now()
    assert now.tzinfo is not None
    assert now.tzinfo == UTC
    
    # Verify it's actually UTC
    assert now.utcoffset().total_seconds() == 0

