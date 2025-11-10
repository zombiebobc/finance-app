"""
Balance override CLI module.

This module provides CLI functions for managing balance overrides,
which allow setting a known balance as of a specific date when historical
transaction data is incomplete.
"""

import logging
import sys
from typing import Optional
from datetime import datetime, date

from database_ops import DatabaseManager
from account_management import AccountManager

logger = logging.getLogger(__name__)


def set_balance_override_cli(
    db_manager: DatabaseManager,
    account_name: str,
    override_date_str: str,
    override_balance: float,
    notes: Optional[str] = None
) -> bool:
    """
    Set a balance override via CLI command.
    
    Args:
        db_manager: DatabaseManager instance
        account_name: Name of account
        override_date_str: Override date as string (YYYY-MM-DD)
        override_balance: Known balance as of override_date
        notes: Optional notes
    
    Returns:
        True if successful, False otherwise
    """
    account_manager = AccountManager(db_manager)
    
    # Get account
    account = account_manager.get_account_by_name(account_name)
    if not account:
        print(f"Error: Account '{account_name}' not found", file=sys.stderr)
        return False
    
    # Get account ID
    account_id = account.id if hasattr(account, 'id') else account['id']
    
    # Parse date
    try:
        override_date = datetime.strptime(override_date_str, '%Y-%m-%d').date()
    except ValueError:
        print(f"Error: Invalid date format '{override_date_str}'. Use YYYY-MM-DD", file=sys.stderr)
        return False
    
    # Validate balance
    try:
        override_balance = float(override_balance)
    except (ValueError, TypeError):
        print("Error: Invalid balance value", file=sys.stderr)
        return False
    
    # Set override
    success = account_manager.set_balance_override(
        account_id=account_id,
        override_date=override_date,
        override_balance=override_balance,
        notes=notes or f"Manual override set via CLI on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    if success:
        # Calculate current balance with override
        current_balance = account_manager.get_balance_with_override(account_id)
        
        print(f"\n[SUCCESS] Balance override set for {account_name}")
        print(f"  Override Date: {override_date}")
        print(f"  Override Balance: ${override_balance:,.2f}")
        print(f"  Current Balance (with transactions after override): ${current_balance:,.2f}")
        if notes:
            print(f"  Notes: {notes}")
        return True
    else:
        print(f"[ERROR] Failed to set balance override for {account_name}", file=sys.stderr)
        return False


def list_balance_overrides_cli(
    db_manager: DatabaseManager,
    account_name: str
) -> None:
    """
    List all balance overrides for an account.
    
    Args:
        db_manager: DatabaseManager instance
        account_name: Name of account
    """
    account_manager = AccountManager(db_manager)
    
    # Get account
    account = account_manager.get_account_by_name(account_name)
    if not account:
        print(f"Error: Account '{account_name}' not found", file=sys.stderr)
        return
    
    # Get account ID
    account_id = account.id if hasattr(account, 'id') else account['id']
    
    # Get overrides
    overrides = account_manager.get_balance_overrides(account_id)
    
    if not overrides:
        print(f"\nNo balance overrides found for {account_name}")
        return
    
    # Display
    print(f"\n{'='*100}")
    print(f"BALANCE OVERRIDES: {account_name}")
    print(f"{'='*100}")
    print(f"{'ID':<5} {'Date':<12} {'Balance':>15} {'Created':>20} {'Notes':<40}")
    print(f"{'-'*100}")
    
    for override in overrides:
        override_id = override['id']
        override_date = override['override_date'].strftime('%Y-%m-%d')
        balance = f"${override['override_balance']:,.2f}"
        created = override['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        notes = override['notes'] or ""
        print(f"{override_id:<5} {override_date:<12} {balance:>15} {created:>20} {notes:<40}")
    
    print(f"{'='*100}")
    print(f"Total overrides: {len(overrides)}")
    
    # Show current balance with override
    current_balance = account_manager.get_balance_with_override(account_id)
    print(f"Current balance (with override): ${current_balance:,.2f}\n")


def delete_balance_override_cli(
    db_manager: DatabaseManager,
    override_id: int
) -> bool:
    """
    Delete a balance override.
    
    Args:
        db_manager: DatabaseManager instance
        override_id: Override ID to delete
    
    Returns:
        True if successful, False otherwise
    """
    account_manager = AccountManager(db_manager)
    
    success = account_manager.delete_balance_override(override_id)
    
    if success:
        print(f"[SUCCESS] Deleted balance override {override_id}")
        return True
    else:
        print(f"[ERROR] Failed to delete balance override {override_id}", file=sys.stderr)
        return False


def show_balance_comparison_cli(
    db_manager: DatabaseManager,
    account_name: str
) -> None:
    """
    Show balance comparison with and without overrides.
    
    Args:
        db_manager: DatabaseManager instance
        account_name: Name of account
    """
    account_manager = AccountManager(db_manager)
    
    # Get account
    account = account_manager.get_account_by_name(account_name)
    if not account:
        print(f"Error: Account '{account_name}' not found", file=sys.stderr)
        return
    
    # Get account ID
    account_id = account.id if hasattr(account, 'id') else account['id']
    account_balance = account.balance if hasattr(account, 'balance') else account['balance']
    
    # Get balance with override
    balance_with_override = account_manager.get_balance_with_override(account_id)
    
    # Get overrides
    overrides = account_manager.get_balance_overrides(account_id)
    
    print(f"\n{'='*80}")
    print(f"BALANCE COMPARISON: {account_name}")
    print(f"{'='*80}")
    print(f"{'Stored Balance (in DB):':<40} ${account_balance:>15,.2f}")
    print(f"{'Calculated Balance (with overrides):':<40} ${balance_with_override:>15,.2f}")
    print(f"{'Difference:':<40} ${balance_with_override - account_balance:>15,.2f}")
    print(f"{'='*80}")
    
    if overrides:
        latest_override = overrides[0]
        print(f"\nLatest Override:")
        print(f"  Date: {latest_override['override_date']}")
        print(f"  Balance: ${latest_override['override_balance']:,.2f}")
        if latest_override['notes']:
            print(f"  Notes: {latest_override['notes']}")
    else:
        print(f"\nNo overrides set for this account.")
    
    print()

