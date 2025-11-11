"""
Fix Robinhood Gold Card payment and refund signs in the database.

This script ONLY inverts the sign of PAYMENTS and REFUNDS for the Robinhood account.
Purchases are left alone as they were already fixed previously.

For Robinhood CSV format:
- Payments in CSV: negative (need to be inverted to positive)
- Refunds in CSV: negative (need to be inverted to positive)
- Purchases in CSV: positive (were already inverted to negative in a previous fix)
"""

import logging
from pathlib import Path
import yaml

from database_ops import DatabaseManager, Transaction, Account, AccountType
from utils import ensure_data_dir, resolve_connection_string

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Account to fix
ACCOUNT_NAME = "Robinhood Gold Card"


def _load_config() -> dict:
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path, "r") as handle:
            return yaml.safe_load(handle) or {}
    return {}


def fix_robinhood_payments(db_manager: DatabaseManager, dry_run: bool = True) -> None:
    """
    Invert signs for ONLY payments and refunds in Robinhood account.
    
    Args:
        db_manager: Database manager instance
        dry_run: If True, show what would be changed without actually changing it
    """
    session = db_manager.get_session()
    
    try:
        # Get the Robinhood account
        account = session.query(Account).filter(
            Account.name == ACCOUNT_NAME
        ).first()
        
        if not account:
            logger.error(f"Account '{ACCOUNT_NAME}' not found")
            return
        
        logger.info(f"Found account: {account.name} (ID: {account.id})")
        
        # Get ONLY payments and refunds based on description patterns
        # Payments and refunds in the Robinhood CSV are negative, so they're currently negative in DB
        # We need to invert them to positive (to reduce debt)
        # Purchases should remain negative (they increase debt)
        transactions = session.query(Transaction).filter(
            Transaction.account_id == account.id,
            Transaction.amount < 0,  # Only negative amounts
            (Transaction.description.like('%Payment%') | Transaction.description.like('%Refund%'))
        ).all()
        
        logger.info(f"\nFound {len(transactions)} negative transactions (payments/refunds)")
        
        if dry_run:
            logger.info("\n*** DRY RUN MODE - No changes will be made ***\n")
            
            # Show what will change
            logger.info("Transactions that will be inverted:")
            for trans in transactions:
                logger.info(
                    f"  {trans.date.date()} | {trans.description[:40]:<40} | "
                    f"${trans.amount:>10.2f} -> ${-trans.amount:>10.2f}"
                )
        else:
            logger.info("\n*** UPDATING DATABASE ***\n")
            
            # Invert all negative transaction amounts (make them positive)
            updated_count = 0
            for trans in transactions:
                old_amount = trans.amount
                trans.amount = -old_amount
                updated_count += 1
                
                logger.info(
                    f"  Updated: {trans.date.date()} | {trans.description[:40]:<40} | "
                    f"${old_amount:>10.2f} -> ${trans.amount:>10.2f}"
                )
            
            # Commit changes
            session.commit()
            logger.info(f"\n✓ Successfully updated {updated_count} transactions")
            
            # Update account balance
            from account_management import AccountManager
            account_manager = AccountManager(db_manager)
            
            logger.info("\nRecalculating account balance...")
            balance = account_manager.get_balance_with_override(account.id)
            logger.info(f"  {account.name}: ${balance:,.2f}")
            
            logger.info("\nVerifying: This should be NEGATIVE (it's a debt)")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error fixing payments: {e}", exc_info=True)
        raise
    finally:
        session.close()


if __name__ == "__main__":
    import sys
    
    # Check for flags
    dry_run = "--apply" not in sys.argv
    force = "--force" in sys.argv
    
    if dry_run:
        print("\n" + "="*80)
        print("DRY RUN MODE - No changes will be made")
        print("Run with --apply flag to actually update the database")
        print("="*80 + "\n")
    else:
        print("\n" + "="*80)
        print("APPLY MODE - Database will be updated!")
        print("Only PAYMENTS and REFUNDS will be inverted (purchases will stay negative)")
        print("="*80 + "\n")
        
        if not force:
            response = input("Are you sure you want to proceed? (yes/no): ")
            if response.lower() != "yes":
                print("Aborted.")
                sys.exit(0)
    
    config = _load_config()
    ensure_data_dir(config)
    connection_string = resolve_connection_string(config)
    
    db_manager = DatabaseManager(connection_string)
    
    try:
        fix_robinhood_payments(db_manager, dry_run=dry_run)
    finally:
        db_manager.close()
    
    if dry_run:
        print("\n" + "="*80)
        print("DRY RUN COMPLETE - No changes were made")
        print("Run with --apply flag to actually update the database")
        print("="*80 + "\n")

