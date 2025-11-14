"""
Unified exception hierarchy for the finance-app project.

This module defines a comprehensive exception hierarchy with FinanceAppError
as the base exception, allowing for consistent error handling across the
application and extensibility for future modules.
"""

from typing import Optional


class FinanceAppError(Exception):
    """
    Base exception class for all finance-app errors.
    
    All custom exceptions in the application should inherit from this class
    to enable unified error handling and consistent error messages.
    
    Attributes:
        message: Human-readable error message
        details: Optional dictionary with additional error context
        original_error: Optional original exception that caused this error
    """
    
    def __init__(
        self,
        message: str,
        details: Optional[dict] = None,
        original_error: Optional[Exception] = None
    ) -> None:
        """
        Initialize FinanceAppError.
        
        Args:
            message: Human-readable error message
            details: Optional dictionary with additional error context
            original_error: Optional original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.original_error = original_error
    
    def __str__(self) -> str:
        """Return string representation of the error."""
        msg = self.message
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            msg = f"{msg} ({detail_str})"
        return msg


class ConfigError(FinanceAppError):
    """Raised when configuration loading or validation fails."""
    pass


class DatabaseError(FinanceAppError):
    """Raised when database operations fail."""
    pass


class IngestionError(FinanceAppError):
    """Raised when CSV ingestion encounters a non-recoverable error."""
    pass


class StandardizationError(FinanceAppError):
    """Raised when data standardization cannot continue safely."""
    pass


class UIError(FinanceAppError):
    """Raised when UI operations fail (Streamlit or CLI rendering issues)."""
    pass


class EncryptionError(FinanceAppError):
    """Base error for encryption failures."""
    pass


class EncryptionKeyError(EncryptionError):
    """Raised when encryption key loading or validation fails."""
    pass


class DecryptionError(EncryptionError):
    """Raised when decryption fails."""
    pass


class DuplicateDetectionError(FinanceAppError):
    """Raised when duplicate detection operations fail."""
    pass


class ImportProcessError(FinanceAppError):
    """Raised when enhanced import operations fail."""
    pass


class CategorizationError(FinanceAppError):
    """Raised when transaction categorization fails."""
    pass


class AccountError(FinanceAppError):
    """Raised when account management operations fail."""
    pass


class BudgetError(FinanceAppError):
    """Raised when budget management operations fail."""
    pass


class AnalyticsError(FinanceAppError):
    """Raised when analytics operations fail."""
    pass


class ReportError(FinanceAppError):
    """Raised when report generation fails."""
    pass


class ViewerError(UIError):
    """Raised when viewer operations fail (CLI or web viewer)."""
    pass


# Extensibility: Future modules can add their own exceptions
# Example:
# class AuthError(FinanceAppError):
#     """Raised when authentication fails."""
#     pass

