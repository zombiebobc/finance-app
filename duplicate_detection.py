"""
Duplicate detection module for financial transactions.

This module generates unique hash keys from transaction key fields (date, description, amount)
and provides functionality to check for duplicates in the database.
"""

import hashlib
import logging
from typing import Dict, Any, List, Set
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class DuplicateDetector:
    """
    Detects duplicate transactions based on exact matches of key fields.
    
    Duplicates are identified by hashing a combination of date, description, and amount.
    This ensures that transactions with the same date, description, and amount are
    considered duplicates, while partial matches (e.g., same amount but different
    date/description) are allowed.
    """
    
    def __init__(self, key_fields: List[str], hash_algorithm: str = "md5"):
        """
        Initialize the duplicate detector.
        
        Args:
            key_fields: List of field names to use for duplicate detection
                (e.g., ['date', 'description', 'amount'])
            hash_algorithm: Hash algorithm to use ('md5', 'sha256', etc.)
        
        Raises:
            ValueError: If hash algorithm is not supported
        """
        self.key_fields = key_fields
        self.hash_algorithm = hash_algorithm.lower()
        
        # Validate hash algorithm
        if self.hash_algorithm not in hashlib.algorithms_available:
            raise ValueError(f"Unsupported hash algorithm: {hash_algorithm}")
        
        logger.info(
            f"Duplicate detector initialized with key fields: {key_fields}, "
            f"algorithm: {hash_algorithm}"
        )
    
    def _normalize_value(self, value: Any) -> str:
        """
        Normalize a value to a string representation for hashing.
        
        Args:
            value: Value to normalize (datetime, float, string, etc.)
        
        Returns:
            Normalized string representation
        """
        if value is None:
            return ""
        elif isinstance(value, datetime):
            # Normalize datetime to ISO format string
            return value.strftime("%Y-%m-%d")
        elif isinstance(value, (int, float)):
            # Normalize numbers to string with consistent formatting
            return f"{float(value):.2f}"
        else:
            # Convert to string and strip whitespace, convert to lowercase
            return str(value).strip().lower()
    
    def generate_hash(self, transaction: Dict[str, Any]) -> str:
        """
        Generate a unique hash for a transaction based on key fields.
        
        Args:
            transaction: Dictionary containing transaction data with key fields
        
        Returns:
            Hexadecimal hash string
        
        Raises:
            KeyError: If required key fields are missing from transaction
        """
        # Collect values for key fields
        hash_parts = []
        for field in self.key_fields:
            if field not in transaction:
                raise KeyError(f"Required key field '{field}' not found in transaction")
            value = transaction[field]
            normalized = self._normalize_value(value)
            hash_parts.append(f"{field}:{normalized}")
        
        # Concatenate all parts with a delimiter
        hash_string = "|".join(hash_parts)
        
        # Generate hash
        hash_obj = hashlib.new(self.hash_algorithm)
        hash_obj.update(hash_string.encode('utf-8'))
        hash_hex = hash_obj.hexdigest()
        
        logger.debug(f"Generated hash '{hash_hex}' for transaction: {hash_string}")
        return hash_hex
    
    def generate_hashes_batch(self, transactions: List[Dict[str, Any]]) -> List[str]:
        """
        Generate hashes for a batch of transactions.
        
        Args:
            transactions: List of transaction dictionaries
        
        Returns:
            List of hash strings (one per transaction)
        
        Note:
            Transactions missing key fields will be skipped with a warning.
        """
        hashes = []
        skipped = 0
        
        for i, transaction in enumerate(transactions):
            try:
                hash_value = self.generate_hash(transaction)
                hashes.append(hash_value)
            except KeyError as e:
                logger.warning(f"Skipping transaction {i} due to missing key field: {e}")
                skipped += 1
                hashes.append(None)  # Keep index alignment
            except Exception as e:
                logger.warning(f"Failed to generate hash for transaction {i}: {e}")
                skipped += 1
                hashes.append(None)
        
        if skipped > 0:
            logger.warning(f"Skipped {skipped} transactions when generating hashes")
        
        return hashes
    
    def filter_duplicates(
        self,
        transactions: List[Dict[str, Any]],
        existing_hashes: Set[str]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Filter out transactions that are duplicates of existing records.
        
        Args:
            transactions: List of transaction dictionaries to check
            existing_hashes: Set of hash strings that already exist in the database
        
        Returns:
            Tuple of (unique_transactions, duplicate_transactions)
        """
        unique_transactions = []
        duplicate_transactions = []
        
        for transaction in transactions:
            try:
                hash_value = self.generate_hash(transaction)
                if hash_value in existing_hashes:
                    duplicate_transactions.append(transaction)
                    logger.debug(f"Found duplicate transaction: {hash_value}")
                else:
                    # Add hash to transaction dict for later use
                    transaction_with_hash = transaction.copy()
                    transaction_with_hash["duplicate_hash"] = hash_value
                    unique_transactions.append(transaction_with_hash)
            except KeyError as e:
                logger.warning(f"Skipping transaction due to missing key field: {e}")
                duplicate_transactions.append(transaction)  # Treat as duplicate to skip
            except Exception as e:
                logger.warning(f"Failed to check duplicate for transaction: {e}")
                duplicate_transactions.append(transaction)
        
        logger.info(
            f"Filtered {len(unique_transactions)} unique transactions, "
            f"{len(duplicate_transactions)} duplicates"
        )
        
        return unique_transactions, duplicate_transactions

