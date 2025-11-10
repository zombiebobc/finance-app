"""
Transaction classification module for detecting transfers and categorizing transactions.

This module provides functions to identify internal transfers between accounts
and prevent them from being counted as expenses in analytics.
"""

import logging
import re
from typing import List, Optional, Dict, Any
import yaml

from database_ops import DatabaseManager, Transaction
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logger = logging.getLogger(__name__)


def load_transfer_patterns(config_path: str = "config.yaml") -> List[str]:
    """
    Load transfer detection patterns from configuration file.
    
    Args:
        config_path: Path to configuration file
    
    Returns:
        List of regex patterns for transfer detection
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        transfer_config = config.get('transfer_detection', {})
        
        if not transfer_config.get('enabled', True):
            logger.info("Transfer detection is disabled in config")
            return []
        
        patterns = transfer_config.get('patterns', [])
        logger.info(f"Loaded {len(patterns)} transfer patterns from config")
        
        return patterns
        
    except Exception as e:
        logger.error(f"Failed to load transfer patterns: {e}")
        # Return some default patterns
        return [
            "Credit Crd-Pay",
            "EDI PMYTS",
            "DEBIT PMTS",
            "Transfer to",
            "Transfer from",
            "Payment to Robinhood"
        ]


def is_transfer(description: str, patterns: Optional[List[str]] = None) -> bool:
    """
    Determine if a transaction description indicates an internal transfer.
    
    Uses regex pattern matching against configured transfer patterns.
    Case-insensitive matching.
    
    Args:
        description: Transaction description to check
        patterns: List of regex patterns (if None, loads from config)
    
    Returns:
        True if description matches transfer patterns, False otherwise
    
    Examples:
        >>> is_transfer("Credit Crd-Pay", ["Credit Crd-Pay"])
        True
        >>> is_transfer("Starbucks Coffee", ["Credit Crd-Pay"])
        False
    """
    if not description:
        return False
    
    # Load patterns if not provided
    if patterns is None:
        patterns = load_transfer_patterns()
    
    if not patterns:
        return False
    
    # Check each pattern
    for pattern in patterns:
        try:
            # Case-insensitive regex match
            if re.search(pattern, description, re.IGNORECASE):
                logger.debug(f"Transfer detected: '{description}' matches pattern '{pattern}'")
                return True
        except re.error as e:
            logger.warning(f"Invalid regex pattern '{pattern}': {e}")
            continue
    
    return False


def is_credit_card_payment(
    description: str,
    account_type: Optional[str] = None,
    account_name: Optional[str] = None
) -> bool:
    """
    Detect if a transaction is a credit card payment based on description and account context.
    
    This is a safeguard function that checks for patterns specific to credit card payments
    that might not be caught by generic transfer patterns.
    
    Args:
        description: Transaction description
        account_type: Account type (e.g., 'credit', 'bank', 'investment')
        account_name: Account name
    
    Returns:
        True if this appears to be a credit card payment, False otherwise
    """
    if not description:
        return False
    
    # Patterns that indicate credit card payments
    credit_payment_patterns = [
        r"^Payment$",  # Just "Payment"
        r"^Payment\s*-",  # Payment with dash
        r"^Payment\s+Received",
        r"^Payment\s+Thank You",
        r"^Automatic Payment",
        r"^AutoPay",
        r"Card Payment",
        r"Credit Card Payment",
    ]
    
    # Check if description matches credit card payment patterns
    for pattern in credit_payment_patterns:
        try:
            if re.search(pattern, description, re.IGNORECASE):
                logger.debug(f"Credit card payment detected: '{description}' matches '{pattern}'")
                return True
        except re.error as e:
            logger.warning(f"Invalid regex pattern '{pattern}': {e}")
            continue
    
    # Additional check: If from a credit account and description contains "payment"
    if account_type:
        # Handle both string and AccountType enum
        account_type_str = str(account_type).lower() if hasattr(account_type, '__str__') else account_type.lower()
        # AccountType enum might be like "AccountType.CREDIT", so check if 'credit' is in the string
        if 'credit' in account_type_str:
            if re.search(r'\bpayment\b', description, re.IGNORECASE):
                logger.debug(f"Credit card payment detected: 'payment' in description from credit account")
                return True
    
    return False


def classify_transaction(
    transaction: Transaction,
    patterns: Optional[List[str]] = None,
    transfer_category: str = "Transfer"
) -> bool:
    """
    Classify a single transaction and update its is_transfer flag.
    
    Args:
        transaction: Transaction object to classify
        patterns: List of regex patterns (if None, loads from config)
        transfer_category: Category to assign to transfers
    
    Returns:
        True if transaction was classified as transfer, False otherwise
    """
    is_xfer = is_transfer(transaction.description, patterns)
    
    if is_xfer:
        transaction.is_transfer = 1
        if transfer_category and not transaction.category:
            transaction.category = transfer_category
        logger.debug(f"Classified as transfer: {transaction.description}")
    
    return is_xfer


def batch_classify_transfers(
    db_manager: DatabaseManager,
    config_path: str = "config.yaml",
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Batch classify all transactions in database to detect transfers.
    
    Updates is_transfer flag for all transactions matching transfer patterns.
    Useful for reclassifying existing transactions after pattern updates.
    
    Args:
        db_manager: Database manager instance
        config_path: Path to configuration file
        dry_run: If True, only count matches without updating database
    
    Returns:
        Dictionary with statistics:
        - 'total': Total transactions processed
        - 'transfers_found': Number of transfers detected
        - 'updated': Number of records updated (0 if dry_run)
        - 'errors': Number of errors encountered
    
    Example:
        >>> stats = batch_classify_transfers(db_manager)
        >>> print(f"Found {stats['transfers_found']} transfers")
    """
    # Load config
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {'total': 0, 'transfers_found': 0, 'updated': 0, 'errors': 1}
    
    transfer_config = config.get('transfer_detection', {})
    
    if not transfer_config.get('enabled', True):
        logger.warning("Transfer detection is disabled in config")
        return {'total': 0, 'transfers_found': 0, 'updated': 0, 'errors': 0}
    
    patterns = transfer_config.get('patterns', [])
    transfer_category = transfer_config.get('transfer_category', 'Transfer')
    log_transfers = transfer_config.get('log_detected_transfers', True)
    
    logger.info(f"Starting batch classification with {len(patterns)} patterns")
    if dry_run:
        logger.info("DRY RUN MODE - No changes will be saved")
    
    session = db_manager.get_session()
    stats = {
        'total': 0,
        'transfers_found': 0,
        'updated': 0,
        'errors': 0
    }
    
    try:
        # Get all transactions
        transactions = session.query(Transaction).all()
        stats['total'] = len(transactions)
        
        logger.info(f"Processing {stats['total']} transactions")
        
        for trans in transactions:
            try:
                # Get account information for enhanced detection
                account_type = None
                account_name = None
                if trans.account_ref:
                    account_type = trans.account_ref.type
                    account_name = trans.account_ref.name
                
                # Check if this is a transfer (pattern matching)
                is_transfer_match = is_transfer(trans.description, patterns)
                
                # Additional safeguard: Check for credit card payments
                is_cc_payment = is_credit_card_payment(
                    trans.description,
                    account_type=account_type,
                    account_name=account_name
                )
                
                # Mark as transfer if either check passes
                if is_transfer_match or is_cc_payment:
                    stats['transfers_found'] += 1
                    
                    # Log if configured
                    if log_transfers:
                        detection_method = "pattern" if is_transfer_match else "credit card safeguard"
                        logger.info(
                            f"Transfer detected ({detection_method}): {trans.date.date()} | "
                            f"{trans.description} | ${trans.amount:.2f} | Account: {account_name or 'Unknown'}"
                        )
                    
                    # Update if not dry run
                    if not dry_run:
                        # Only update if not already marked
                        if trans.is_transfer == 0:
                            trans.is_transfer = 1
                            
                            # Optionally update category
                            if transfer_category and not trans.category:
                                trans.category = transfer_category
                            
                            stats['updated'] += 1
                
            except Exception as e:
                logger.error(f"Error processing transaction {trans.id}: {e}")
                stats['errors'] += 1
                continue
        
        # Commit changes if not dry run
        if not dry_run and stats['updated'] > 0:
            session.commit()
            logger.info(f"Updated {stats['updated']} transactions")
        
        logger.info(
            f"Batch classification complete: "
            f"{stats['transfers_found']} transfers found out of {stats['total']} transactions"
        )
        
    except SQLAlchemyError as e:
        logger.error(f"Database error during batch classification: {e}")
        session.rollback()
        stats['errors'] += 1
    
    finally:
        session.close()
    
    return stats


def get_transfer_statistics(db_manager: DatabaseManager) -> Dict[str, Any]:
    """
    Get statistics about transfers in the database.
    
    Args:
        db_manager: Database manager instance
    
    Returns:
        Dictionary with transfer statistics:
        - 'total_transactions': Total number of transactions
        - 'total_transfers': Number of transactions marked as transfers
        - 'transfer_percentage': Percentage of transactions that are transfers
        - 'transfer_amount_total': Total amount of transfers
    """
    from sqlalchemy import func
    
    session = db_manager.get_session()
    
    try:
        # Get total transaction count
        total = session.query(func.count(Transaction.id)).scalar() or 0
        
        # Get transfer count
        transfer_count = session.query(func.count(Transaction.id)).filter(
            Transaction.is_transfer == 1
        ).scalar() or 0
        
        # Get transfer amount total
        transfer_amount = session.query(func.sum(Transaction.amount)).filter(
            Transaction.is_transfer == 1
        ).scalar() or 0.0
        
        # Calculate percentage
        percentage = (transfer_count / total * 100) if total > 0 else 0
        
        return {
            'total_transactions': total,
            'total_transfers': transfer_count,
            'transfer_percentage': round(percentage, 2),
            'transfer_amount_total': round(transfer_amount, 2)
        }
        
    except SQLAlchemyError as e:
        logger.error(f"Error getting transfer statistics: {e}")
        return {
            'total_transactions': 0,
            'total_transfers': 0,
            'transfer_percentage': 0,
            'transfer_amount_total': 0
        }
    
    finally:
        session.close()


def manual_reclassify(
    db_manager: DatabaseManager,
    transaction_id: int,
    is_transfer_flag: bool
) -> bool:
    """
    Manually reclassify a transaction's transfer status.
    
    Useful for correcting false positives or false negatives.
    
    Args:
        db_manager: Database manager instance
        transaction_id: ID of transaction to reclassify
        is_transfer_flag: True to mark as transfer, False to mark as regular transaction
    
    Returns:
        True if successful, False otherwise
    """
    session = db_manager.get_session()
    
    try:
        # Get transaction
        trans = session.query(Transaction).filter(
            Transaction.id == transaction_id
        ).first()
        
        if not trans:
            logger.error(f"Transaction {transaction_id} not found")
            return False
        
        # Update flag
        old_value = trans.is_transfer
        trans.is_transfer = 1 if is_transfer_flag else 0
        
        session.commit()
        
        logger.info(
            f"Manually reclassified transaction {transaction_id}: "
            f"is_transfer {old_value} -> {trans.is_transfer}"
        )
        
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"Error reclassifying transaction: {e}")
        session.rollback()
        return False
    
    finally:
        session.close()


if __name__ == "__main__":
    # Test transfer detection
    test_descriptions = [
        "Credit Crd-Pay",
        "Starbucks Coffee",
        "EDI PMYTS PAYMENT",
        "Transfer to Robinhood",
        "Grocery Store Purchase",
        "DEBIT PMTS",
        "Transfer to Savings"
    ]
    
    patterns = load_transfer_patterns()
    
    print("Transfer Detection Test:")
    print("=" * 50)
    for desc in test_descriptions:
        result = is_transfer(desc, patterns)
        print(f"{desc:<40} {'TRANSFER' if result else 'EXPENSE'}")

