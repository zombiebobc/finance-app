"""
Fix Robinhood Gold Card payment and refund signs in the database.

This script ONLY inverts the sign of PAYMENTS and REFUNDS for the Robinhood account.
Purchases are left alone as they were already fixed previously.

For Robinhood CSV format:
- Payments in CSV: negative (need to be inverted to positive)
- Refunds in CSV: negative (need to be inverted to positive)
- Purchases in CSV: positive (were already inverted to negative in a previous fix)
"""

import argparse
import logging
from pathlib import Path
from typing import Iterable

import yaml

from database_ops import DatabaseManager, Transaction, Account
from utils import ensure_data_dir, resolve_connection_string

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_ACCOUNT_NAME = "Robinhood Gold Card"


def _load_config() -> dict:
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path, "r") as handle:
            return yaml.safe_load(handle) or {}
    return {}


def _format_change(trans: Transaction, new_amount: float) -> str:
    return (
        f"  {trans.date.date()} | {trans.description[:40]:<40} | "
        f"${trans.amount:>10.2f} -> ${new_amount:>10.2f}"
    )


def _invert_transactions(transactions: Iterable[Transaction], dry_run: bool) -> int:
    updated_count = 0
    for trans in transactions:
        new_amount = -trans.amount
        if dry_run:
            logger.info(_format_change(trans, new_amount))
        else:
            trans.amount = new_amount
            updated_count += 1
            logger.info(_format_change(trans, new_amount))
    return updated_count


def fix_robinhood_transactions(
    db_manager: DatabaseManager,
    account_name: str,
    *,
    fix_purchases: bool = True,
    fix_payments: bool = False,
    dry_run: bool = True
) -> None:
    """
    Reconcile transaction signs for a Robinhood (or similar) credit account.
    
    Args:
        db_manager: Active database manager.
        account_name: Target account name.
        fix_purchases: When True, invert any positive amounts (purchases) to negative.
        fix_payments: When True, invert negative Payment/Refund rows to positive.
        dry_run: If True, only report the changes that would be made.
    """
    session = db_manager.get_session()
    try:
        account = session.query(Account).filter(Account.name == account_name).first()
        if not account:
            logger.error("Account '%s' not found", account_name)
            return
        
        logger.info("Fixing transactions for account: %s (ID: %s)", account.name, account.id)
        total_updates = 0
        
        if fix_purchases:
            purchases = session.query(Transaction).filter(
                Transaction.account_id == account.id,
                Transaction.amount > 0
            ).all()
            logger.info("Found %s positive transactions (purchases) to invert", len(purchases))
            total_updates += _invert_transactions(purchases, dry_run)
        
        if fix_payments:
            payments = session.query(Transaction).filter(
                Transaction.account_id == account.id,
                Transaction.amount < 0,
                (Transaction.description.like('%Payment%') | Transaction.description.like('%Refund%'))
            ).all()
            logger.info("Found %s negative Payment/Refund transactions to invert", len(payments))
            total_updates += _invert_transactions(payments, dry_run)
        
        if dry_run:
            logger.info("\n*** DRY RUN COMPLETE - No changes were made ***\n")
            return
        
        if total_updates == 0:
            logger.info("No transactions required updates.")
            return
        
        session.commit()
        logger.info("✓ Successfully updated %s transactions", total_updates)
        
        from account_management import AccountManager
        account_manager = AccountManager(db_manager)
        logger.info("Recalculating account balance...")
        balance = account_manager.get_balance_with_override(account.id)
        logger.info("  %s: $%s", account.name, f"{balance:,.2f}")
    except Exception as exc:  # pragma: no cover - defensive
        session.rollback()
        logger.error("Error adjusting transactions: %s", exc, exc_info=True)
        raise
    finally:
        session.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fix Robinhood transaction signs.")
    parser.add_argument(
        "--account",
        default=DEFAULT_ACCOUNT_NAME,
        help="Account name to correct (default: %(default)s)"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes to the database (default: dry run)."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt when applying changes."
    )
    parser.add_argument(
        "--skip-purchases",
        action="store_true",
        help="Do not invert positive purchases."
    )
    parser.add_argument(
        "--include-payments",
        action="store_true",
        help="Also invert negative Payment/Refund rows."
    )
    return parser


if __name__ == "__main__":
    parser = _build_parser()
    args = parser.parse_args()
    
    dry_run = not args.apply
    if dry_run:
        print("\n" + "=" * 80)
        print("DRY RUN MODE - No changes will be made")
        print("Run with --apply to update the database (use --force to skip the prompt)")
        print("=" * 80 + "\n")
    elif not args.force:
        response = input("Apply changes to the database? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            raise SystemExit(0)
    
    config = _load_config()
    ensure_data_dir(config)
    connection_string = resolve_connection_string(config)
    db_manager = DatabaseManager(connection_string)
    
    try:
        fix_robinhood_transactions(
            db_manager,
            args.account,
            fix_purchases=not args.skip_purchases,
            fix_payments=args.include_payments,
            dry_run=dry_run
        )
    finally:
        db_manager.close()

