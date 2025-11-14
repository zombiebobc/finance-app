"""
Unit tests for the unified exception hierarchy.

Tests exception creation, attributes, string representations, and error context.
"""

import pytest
from exceptions import (
    FinanceAppError,
    ConfigError,
    DatabaseError,
    IngestionError,
    StandardizationError,
    UIError,
    EncryptionError,
    EncryptionKeyError,
    DecryptionError,
    DuplicateDetectionError,
    ImportProcessError,
    CategorizationError,
    AccountError,
    BudgetError,
    AnalyticsError,
    ReportError,
    ViewerError
)


class TestFinanceAppError:
    """Test base FinanceAppError class."""
    
    def test_basic_exception_creation(self):
        """Test creating a basic FinanceAppError."""
        error = FinanceAppError("Test error message")
        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.details == {}
        assert error.original_error is None
    
    def test_exception_with_details(self):
        """Test creating exception with details dictionary."""
        details = {"key1": "value1", "key2": 123}
        error = FinanceAppError("Test error", details=details)
        assert error.message == "Test error"
        assert error.details == details
        assert "key1=value1" in str(error)
        assert "key2=123" in str(error)
    
    def test_exception_with_original_error(self):
        """Test creating exception with original error chaining."""
        original = ValueError("Original error")
        error = FinanceAppError("Wrapped error", original_error=original)
        assert error.message == "Wrapped error"
        assert error.original_error == original
        assert isinstance(error.original_error, ValueError)
    
    def test_exception_inheritance(self):
        """Test that FinanceAppError is a subclass of Exception."""
        error = FinanceAppError("Test")
        assert isinstance(error, Exception)
        assert isinstance(error, FinanceAppError)


class TestExceptionHierarchy:
    """Test specific exception subclasses."""
    
    def test_config_error(self):
        """Test ConfigError creation and attributes."""
        error = ConfigError(
            "Config file not found",
            details={"config_path": "/path/to/config.yaml"}
        )
        assert isinstance(error, FinanceAppError)
        assert error.message == "Config file not found"
        assert error.details["config_path"] == "/path/to/config.yaml"
    
    def test_database_error(self):
        """Test DatabaseError creation and attributes."""
        original = ConnectionError("Connection failed")
        error = DatabaseError(
            "Database operation failed",
            details={"operation": "insert", "table": "transactions"},
            original_error=original
        )
        assert isinstance(error, FinanceAppError)
        assert error.original_error == original
        assert error.details["operation"] == "insert"
    
    def test_ingestion_error(self):
        """Test IngestionError creation."""
        error = IngestionError("CSV file read failed")
        assert isinstance(error, FinanceAppError)
        assert error.message == "CSV file read failed"
    
    def test_standardization_error(self):
        """Test StandardizationError creation."""
        error = StandardizationError(
            "Missing required field",
            details={"missing_field": "amount", "row": 42}
        )
        assert isinstance(error, FinanceAppError)
        assert error.details["missing_field"] == "amount"
    
    def test_ui_error(self):
        """Test UIError creation."""
        error = UIError("UI rendering failed")
        assert isinstance(error, FinanceAppError)
    
    def test_viewer_error(self):
        """Test ViewerError inheritance from UIError."""
        error = ViewerError("Viewer failed")
        assert isinstance(error, UIError)
        assert isinstance(error, FinanceAppError)
    
    def test_encryption_error_hierarchy(self):
        """Test encryption error hierarchy."""
        key_error = EncryptionKeyError("Invalid key")
        assert isinstance(key_error, EncryptionError)
        assert isinstance(key_error, FinanceAppError)
        
        decrypt_error = DecryptionError("Decryption failed")
        assert isinstance(decrypt_error, EncryptionError)
        assert isinstance(decrypt_error, FinanceAppError)
    
    def test_duplicate_detection_error(self):
        """Test DuplicateDetectionError creation."""
        error = DuplicateDetectionError(
            "Unsupported hash algorithm",
            details={"algorithm": "sha999", "available": ["md5", "sha256"]}
        )
        assert isinstance(error, FinanceAppError)
    
    def test_import_process_error(self):
        """Test ImportProcessError creation."""
        error = ImportProcessError("Import failed")
        assert isinstance(error, FinanceAppError)
    
    def test_categorization_error(self):
        """Test CategorizationError creation."""
        error = CategorizationError("Categorization failed")
        assert isinstance(error, FinanceAppError)
    
    def test_account_error(self):
        """Test AccountError creation."""
        error = AccountError(
            "Account already exists",
            details={"account_name": "Test Account", "existing_id": 1}
        )
        assert isinstance(error, FinanceAppError)
    
    def test_budget_error(self):
        """Test BudgetError creation."""
        error = BudgetError("Budget validation failed")
        assert isinstance(error, FinanceAppError)
    
    def test_analytics_error(self):
        """Test AnalyticsError creation."""
        error = AnalyticsError(
            "Invalid time frame",
            details={"time_frame": "invalid", "valid_formats": ["1m", "3m", "6m"]}
        )
        assert isinstance(error, FinanceAppError)
    
    def test_report_error(self):
        """Test ReportError creation."""
        error = ReportError("Report generation failed")
        assert isinstance(error, FinanceAppError)


class TestExceptionStringRepresentation:
    """Test exception string representations."""
    
    def test_simple_message(self):
        """Test simple error message."""
        error = FinanceAppError("Simple error")
        assert str(error) == "Simple error"
    
    def test_message_with_details(self):
        """Test error message with details."""
        error = FinanceAppError(
            "Error with context",
            details={"field": "amount", "value": 123.45}
        )
        assert "Error with context" in str(error)
        assert "field=amount" in str(error) or "value=123.45" in str(error)
    
    def test_exception_inheritance_str(self):
        """Test that subclasses also have proper string representation."""
        error = DatabaseError("DB error", details={"op": "select"})
        assert "DB error" in str(error)


class TestExceptionContext:
    """Test exception context and error chaining."""
    
    def test_error_chaining(self):
        """Test preserving original error in exception chain."""
        original = ValueError("Original")
        wrapped = DatabaseError("Wrapped", original_error=original)
        assert wrapped.original_error == original
        assert isinstance(wrapped.original_error, ValueError)
    
    def test_details_preservation(self):
        """Test that details are preserved across exception hierarchy."""
        details = {
            "operation": "insert",
            "table": "transactions",
            "error_code": "INTEGRITY_ERROR"
        }
        error = DatabaseError("DB error", details=details)
        assert error.details == details
        assert error.details["operation"] == "insert"
    
    def test_empty_details_default(self):
        """Test that empty details dict is created by default."""
        error = FinanceAppError("Test")
        assert error.details == {}
        assert isinstance(error.details, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

