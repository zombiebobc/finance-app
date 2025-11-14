"""
Encryption utilities for symmetric field protection across the finance app.

Provides key management, Fernet-based helpers, SQLAlchemy type decorators, and
SQLite user-defined functions so encrypted columns remain transparent to the
rest of the application code.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import sqlite3
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Type

import yaml
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import event, func
from sqlalchemy.types import String, TypeDecorator

logger = logging.getLogger(__name__)

ENV_KEY_NAME = "FINANCE_APP_ENCRYPTION_KEY"
CONFIG_FILE = Path("config.yaml")
CONFIG_KEY_PATH = ("security", "encryption_key")

# Default column lengths for ciphertext (Fernet expands data by ~33%).
DEFAULT_STRING_LENGTH = 2048
DEFAULT_NUMERIC_LENGTH = 512


class EncryptionError(RuntimeError):
    """Base error for encryption failures."""


class EncryptionKeyError(EncryptionError):
    """Raised when encryption key loading or validation fails."""


class DecryptionError(EncryptionError):
    """Raised when ciphertext cannot be decrypted."""


def _validate_fernet_key(raw_key: str | bytes) -> bytes:
    """Validate and normalize a Fernet key string."""
    key_bytes = raw_key.encode("utf-8") if isinstance(raw_key, str) else raw_key
    try:
        # Validation occurs during instantiation.
        Fernet(key_bytes)
    except (ValueError, TypeError) as exc:
        raise EncryptionKeyError("Invalid Fernet key supplied.") from exc
    return key_bytes


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=True)


class EncryptionManager:
    """
    Centralized Fernet helper that lazily loads or generates the encryption key.

    Key resolution order:
        1. Environment variable FINANCE_APP_ENCRYPTION_KEY
        2. config.yaml -> security.encryption_key
        3. Auto-generated key persisted back to config.yaml (if allowed)
    """

    def __init__(
        self,
        *,
        config_file: Path = CONFIG_FILE,
        env_var: str = ENV_KEY_NAME,
        auto_generate: bool = True,
    ) -> None:
        self.config_file = config_file
        self.env_var = env_var
        self.auto_generate = auto_generate
        self._key: Optional[bytes] = None
        self._fernet: Optional[Fernet] = None

    def _load_from_env(self) -> Optional[bytes]:
        raw_key = os.environ.get(self.env_var)
        if not raw_key:
            return None
        try:
            return _validate_fernet_key(raw_key.strip())
        except EncryptionKeyError as exc:
            raise EncryptionKeyError(
                "Environment encryption key is invalid. Regenerate and re-set the "
                f"{self.env_var} variable."
            ) from exc

    def _load_from_config(self) -> Optional[bytes]:
        try:
            content = _read_yaml(self.config_file)
        except yaml.YAMLError as exc:  # pragma: no cover - defensive
            logger.error("Unable to parse config file for encryption key: %s", exc)
            return None

        section = content.get(CONFIG_KEY_PATH[0], {})
        raw_key = section.get(CONFIG_KEY_PATH[1])
        if not raw_key:
            return None
        try:
            return _validate_fernet_key(raw_key)
        except EncryptionKeyError:
            logger.warning("Invalid encryption key stored in config.yaml; regenerating.")
            return None

    def _persist_key(self, key: bytes) -> None:
        try:
            config_data = _read_yaml(self.config_file)
            security_section = config_data.get(CONFIG_KEY_PATH[0], {})
            security_section[CONFIG_KEY_PATH[1]] = key.decode("utf-8")
            config_data[CONFIG_KEY_PATH[0]] = security_section
            _write_yaml(self.config_file, config_data)
            logger.info("Generated new encryption key and stored it in config.yaml.")
        except Exception as exc:  # pragma: no cover - disk failure
            logger.error("Failed to persist encryption key to config.yaml: %s", exc)
            raise

    def _generate_key(self) -> bytes:
        key = Fernet.generate_key()
        if self.auto_generate:
            self._persist_key(key)
        return key

    def get_key(self) -> bytes:
        if self._key:
            return self._key

        key = self._load_from_env() or self._load_from_config()
        if key is None:
            if not self.auto_generate:
                raise EncryptionKeyError(
                    "Encryption key not found. Provide FINANCE_APP_ENCRYPTION_KEY "
                    "or add security.encryption_key to config.yaml."
                )
            key = self._generate_key()

        self._key = key
        self._fernet = Fernet(key)
        return key

    def _require_fernet(self) -> Fernet:
        if not self._fernet:
            self.get_key()
        if not self._fernet:  # pragma: no cover - satisfy type checker
            raise EncryptionKeyError("Encryption key unavailable.")
        return self._fernet

    def encrypt_value(self, value: Any, python_type: Type[Any] = str) -> Optional[str]:
        """
        Encrypt a single value. Returns base64 ciphertext as string.

        Empty strings and None are returned as-is to honor the "skip empty fields"
        requirement.
        """
        if value is None:
            return None
        if isinstance(value, str) and value == "":
            return value

        normalized: bytes
        if python_type in (float, int):
            normalized = repr(float(value)).encode("utf-8")
        elif python_type is bytes:
            normalized = bytes(value)
        else:
            normalized = str(value).encode("utf-8")

        token = self._require_fernet().encrypt(normalized)
        return token.decode("utf-8")

    def decrypt_value(self, token: Any, python_type: Type[Any] = str) -> Any:
        """
        Decrypt a ciphertext string into the requested python_type.

        Returns None when ciphertext is empty, and logs & raises DecryptionError
        for invalid tokens.
        """
        if token is None:
            return None
        if isinstance(token, str) and token == "":
            return token

        try:
            if isinstance(token, str):
                token_bytes = token.encode("utf-8")
            else:
                token_bytes = bytes(token)
            decrypted = self._require_fernet().decrypt(token_bytes)
        except InvalidToken as exc:
            logger.error("Failed to decrypt payload; token invalid.")
            raise DecryptionError("Unable to decrypt stored value.") from exc

        decoded = decrypted.decode("utf-8")
        try:
            if python_type is float:
                return float(decoded)
            if python_type is int:
                return int(float(decoded))
            if python_type is bytes:
                return decrypted
            return decoded
        except (TypeError, ValueError) as exc:
            logger.error("Decrypted value could not be coerced to %s.", python_type)
            raise DecryptionError("Decrypted value has unexpected format.") from exc


@lru_cache(maxsize=1)
def get_encryption_manager() -> EncryptionManager:
    """Return a singleton EncryptionManager instance."""
    return EncryptionManager()


class _DecryptedComparator(TypeDecorator.Comparator):
    """Comparator that transparently applies the proper decrypt_* SQLite function."""

    def _decrypted_expr(self):
        decrypt_func = getattr(func, self.type.decrypt_func_name)
        return decrypt_func(self.expr)

    def operate(self, op, *other, **kwargs):  # type: ignore[override]
        expr = self._decrypted_expr()
        if other:
            return op(expr, *other, **kwargs)
        return op(expr, **kwargs)

    def reversed_operate(self, op, other, **kwargs):  # type: ignore[override]
        return op(other, self._decrypted_expr(), **kwargs)

    def asc(self):
        return self._decrypted_expr().asc()

    def desc(self):
        return self._decrypted_expr().desc()


class EncryptedType(TypeDecorator):
    """
    Generic SQLAlchemy TypeDecorator that stores ciphertext while exposing plaintext.

    The column is persisted as TEXT while comparators/order/grouping operate on a
    SQLite user-defined function that decrypts values on the fly.
    
    All instances are cache-safe because they use a singleton EncryptionManager
    that doesn't change between queries. This allows SQLAlchemy to cache compiled
    queries efficiently.
    """

    impl = String(DEFAULT_STRING_LENGTH)
    cache_ok = True  # All instances are cache-safe (singleton EncryptionManager)
    comparator_factory = _DecryptedComparator

    def __init__(
        self,
        *,
        python_type: Type[Any] = str,
        length: int | None = None,
        decrypt_func_name: str | None = None,
    ) -> None:
        super().__init__(length or DEFAULT_STRING_LENGTH)
        self._python_type = python_type
        if decrypt_func_name:
            self.decrypt_func_name = decrypt_func_name
        elif python_type in (float, int):
            self.decrypt_func_name = "decrypt_numeric"
        else:
            self.decrypt_func_name = "decrypt_text"
        # Ensure cache_ok remains True after initialization
        # (SQLAlchemy checks instance state, not just class attribute)
        self.cache_ok = True

    @property
    def python_type(self) -> Type[Any]:  # type: ignore[override]
        return self._python_type

    def process_bind_param(self, value: Any, dialect) -> Any:  # type: ignore[override]
        manager = get_encryption_manager()
        return manager.encrypt_value(value, self._python_type)

    def process_result_value(self, value: Any, dialect) -> Any:  # type: ignore[override]
        manager = get_encryption_manager()
        if value is None:
            return None
        try:
            if isinstance(value, str) and not is_ciphertext(value):
                # Legacy plaintext path for backward compatibility.
                if self._python_type is float:
                    return float(value)
                if self._python_type is int:
                    return int(float(value))
                return value
            return manager.decrypt_value(value, self._python_type)
        except DecryptionError:
            # Caller requested default behavior of returning None on failure.
            return None

    def column_expression(self, colexpr):  # type: ignore[override]
        decrypt_func = getattr(func, self.decrypt_func_name)
        return decrypt_func(colexpr)


class EncryptedString(EncryptedType):
    """
    Shortcut for encrypted text columns.
    
    All instances are cache-safe because they use a singleton EncryptionManager.
    """
    
    cache_ok = True  # Explicitly mark as cache-safe for SQLAlchemy 2.x

    def __init__(self, length: int = DEFAULT_STRING_LENGTH):
        super().__init__(python_type=str, length=length, decrypt_func_name="decrypt_text")
        # Ensure cache_ok remains True after initialization
        self.cache_ok = True


class EncryptedNumeric(EncryptedType):
    """
    Shortcut for encrypted numeric columns.
    
    All instances are cache-safe because they use a singleton EncryptionManager.
    """
    
    cache_ok = True  # Explicitly mark as cache-safe for SQLAlchemy 2.x

    def __init__(self, length: int = DEFAULT_NUMERIC_LENGTH):
        super().__init__(python_type=float, length=length, decrypt_func_name="decrypt_numeric")
        # Ensure cache_ok remains True after initialization
        self.cache_ok = True


def register_sqlite_functions(dbapi_connection: sqlite3.Connection) -> None:
    """
    Register SQLite helper functions used by encrypted columns.

    Ensures ORM queries, manual SQL, and raw sqlite3 helpers can project and
    filter on decrypted values while keeping ciphertext on disk.
    """
    manager = get_encryption_manager()

    def decrypt_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        try:
            if not is_ciphertext(value):
                return value
            return manager.decrypt_value(value, str)
        except DecryptionError:
            return None

    def decrypt_numeric(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            if not is_ciphertext(value):
                return float(value)
            decrypted = manager.decrypt_value(value, float)
            return None if decrypted is None else float(decrypted)
        except DecryptionError:
            return None

    try:
        dbapi_connection.create_function("decrypt_text", 1, decrypt_text)
        dbapi_connection.create_function("decrypt_numeric", 1, decrypt_numeric)
    except sqlite3.OperationalError as exc:  # pragma: no cover - driver-specific
        logger.error("Failed to register SQLite encryption helpers: %s", exc)
        raise


def attach_sqlalchemy_listeners(engine) -> None:
    """
    Attach SQLAlchemy event listeners so every SQLite connection registers helpers.
    """

    def _on_connect(dbapi_connection, connection_record):  # pragma: no cover - event hook
        register_sqlite_functions(dbapi_connection)

    event.listen(engine, "connect", _on_connect)


TransactionFieldMap = Dict[str, Type[Any]]

SENSITIVE_TRANSACTION_FIELDS: TransactionFieldMap = {
    "description": str,
    "amount": float,
    "category": str,
    "account": str,
    "source_file": str,
}


def encrypt_transaction_payload(payload: Mapping[str, Any], *, skip_if_encrypted: bool = False) -> Dict[str, Any]:
    """
    Encrypt sensitive transaction fields inside a dict before persistence.
    """
    manager = get_encryption_manager()
    protected = dict(payload)
    for field, field_type in SENSITIVE_TRANSACTION_FIELDS.items():
        if field in protected:
            if skip_if_encrypted and isinstance(protected[field], str) and is_ciphertext(protected[field]):
                continue
            protected[field] = manager.encrypt_value(protected[field], field_type)
    return protected


def decrypt_transaction_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Return a new dict with all sensitive transaction fields decrypted.
    """
    manager = get_encryption_manager()
    result = dict(row)
    for field, field_type in SENSITIVE_TRANSACTION_FIELDS.items():
        if field in result and result[field] is not None:
            try:
                result[field] = manager.decrypt_value(result[field], field_type)
            except DecryptionError:
                result[field] = None
    return result


def is_ciphertext(value: Any) -> bool:
    """Lightweight heuristic to determine if a value is Fernet ciphertext."""
    if not isinstance(value, str):
        return False
    try:
        base64.urlsafe_b64decode(value.encode("utf-8"))
        return True
    except Exception:
        return False


def derive_search_token(value: Optional[str]) -> Optional[str]:
    """
    Generate a deterministic HMAC token for indexing/uniqueness checks.

    Returns None for empty values to avoid storing meaningless hashes.
    """
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    key = get_encryption_manager().get_key()
    digest = hmac.new(key, normalized.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8")


