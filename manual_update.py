"""
Manual balance update module for investment and savings accounts.

This module provides functions for manually updating account balances,
typically used for investment accounts where balances cannot be automatically
calculated from transaction history.
"""

import logging
from typing import Optional
import sys

from database_ops import DatabaseManager
from account_management import AccountManager

logger = logging.getLogger(__name__)


def prompt_balance_update_cli(
    account_manager: AccountManager,
    account_name: str,
    current_balance: Optional[float] = None
) -> bool:
    """
    Prompt user in CLI to update account balance.
    
    Args:
        account_manager: AccountManager instance
        account_name: Name of account to update
        current_balance: Optional current balance to display
    
    Returns:
        True if balance was updated, False otherwise
    """
    # Get account
    account = account_manager.get_account_by_name(account_name)
    if not account:
        logger.error(f"Account '{account_name}' not found")
        return False
    
    # Get account details (handle both dict and Account object)
    account_id = account.id if hasattr(account, 'id') else account['id']
    account_balance = account.balance if hasattr(account, 'balance') else account['balance']
    
    # Ask if user wants to update
    print(f"\n{'='*60}")
    print(f"Investment Balance Update: {account_name}")
    print(f"{'='*60}")
    
    if current_balance is not None:
        print(f"Current recorded balance: ${current_balance:,.2f}")
    else:
        print(f"Current recorded balance: ${account_balance:,.2f}")
    
    response = input("\nWould you like to update this balance? [y/N]: ").strip().lower()
    
    if response not in ['y', 'yes']:
        print("Balance update skipped.")
        return False
    
    # Get new balance with validation
    while True:
        try:
            balance_input = input("\nEnter new balance (e.g., 12345.67): $").strip()
            new_balance = float(balance_input)
            
            # Allow negative balances (losses are possible)
            if abs(new_balance) > 1_000_000_000:  # Sanity check
                print("Error: Balance seems unreasonably large. Please re-enter.")
                continue
            
            break
        except ValueError:
            print("Error: Invalid number format. Please enter a valid amount (e.g., 12345.67)")
        except KeyboardInterrupt:
            print("\n\nBalance update cancelled.")
            return False
    
    # Get optional notes
    notes = input("Optional notes about this balance (press Enter to skip): ").strip()
    if not notes:
        notes = "Manual update via CLI"
    
    # Update balance
    success = account_manager.update_balance(
        account_id=account_id,
        new_balance=new_balance,
        notes=notes
    )
    
    if success:
        print(f"\n[SUCCESS] Balance updated successfully: ${new_balance:,.2f}")
        return True
    else:
        print("\n[ERROR] Failed to update balance. Check logs for details.")
        return False


def update_balance_cli(
    db_manager: DatabaseManager,
    account_name: str,
    balance: float,
    notes: Optional[str] = None
) -> bool:
    """
    Update account balance via CLI command (non-interactive).
    
    Args:
        db_manager: DatabaseManager instance
        account_name: Name of account to update
        balance: New balance value
        notes: Optional notes about the update
    
    Returns:
        True if successful, False otherwise
    """
    account_manager = AccountManager(db_manager)
    
    # Get account
    account = account_manager.get_account_by_name(account_name)
    if not account:
        print(f"Error: Account '{account_name}' not found", file=sys.stderr)
        return False
    
    # Get account ID (handle both dict and Account object)
    account_id = account.id if hasattr(account, 'id') else account['id']
    
    # Validate balance
    try:
        balance = float(balance)
        if abs(balance) > 1_000_000_000:
            print("Error: Balance seems unreasonably large", file=sys.stderr)
            return False
    except (ValueError, TypeError):
        print("Error: Invalid balance value", file=sys.stderr)
        return False
    
    # Update
    success = account_manager.update_balance(
        account_id=account_id,
        new_balance=balance,
        notes=notes or "Manual update via CLI command"
    )
    
    if success:
        print(f"[SUCCESS] Updated {account_name} balance to ${balance:,.2f}")
        return True
    else:
        print(f"[ERROR] Failed to update balance for {account_name}", file=sys.stderr)
        return False


def show_balance_history(
    db_manager: DatabaseManager,
    account_name: str,
    limit: int = 10
) -> None:
    """
    Display balance history for an account.
    
    Args:
        db_manager: DatabaseManager instance
        account_name: Name of account
        limit: Number of entries to show
    """
    account_manager = AccountManager(db_manager)
    
    # Get account
    account = account_manager.get_account_by_name(account_name)
    if not account:
        print(f"Error: Account '{account_name}' not found", file=sys.stderr)
        return
    
    # Get account ID (handle both dict and Account object)
    account_id = account.id if hasattr(account, 'id') else account['id']
    
    # Get history
    history = account_manager.get_balance_history(account_id, limit=limit)
    
    if not history:
        print(f"\nNo balance history found for {account_name}")
        return
    
    # Display
    print(f"\n{'='*80}")
    print(f"BALANCE HISTORY: {account_name}")
    print(f"{'='*80}")
    print(f"{'Timestamp':<20} {'Balance':>15} {'Notes':<40}")
    print(f"{'-'*80}")
    
    for entry in history:
        timestamp_str = entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        balance_str = f"${entry['balance']:,.2f}"
        notes_str = entry['notes'] or ""
        print(f"{timestamp_str:<20} {balance_str:>15} {notes_str:<40}")
    
    print(f"{'='*80}")
    print(f"Showing {len(history)} most recent entries")
    if len(history) == limit:
        print(f"(Use --limit to show more)")
    print()


def detect_wealthfront_transfers(transactions: list, config: dict) -> list:
    """
    Detect Wealthfront transfers to investment account based on description patterns.
    
    Args:
        transactions: List of transaction dictionaries
        config: Configuration dictionary with transfer patterns
    
    Returns:
        List of transactions that are transfers
    """
    import re
    
    # Get transfer patterns from config
    patterns = config.get('wealthfront', {}).get('transfer_patterns', [
        r'[Tt]ransfer\s+to\s+[Aa]utomated\s+[Ii]nvesting',
        r'[Tt]ransfer\s+to\s+[Ii]nvestment',
        r'[Ww]ealthfront\s+[Ii]nvestment',
        r'[Aa]uto-[Ii]nvest'
    ])
    
    transfers = []
    
    for trans in transactions:
        description = trans.get('description', '')
        
        # Check if description matches any transfer pattern
        for pattern in patterns:
            if re.search(pattern, description, re.IGNORECASE):
                transfers.append(trans)
                logger.debug(f"Detected transfer: {description}")
                break
    
    logger.info(f"Detected {len(transfers)} Wealthfront transfers")
    return transfers

