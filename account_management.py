"""
Account management module for financial accounts.

This module provides CRUD operations for managing financial accounts,
including banks, credit cards, investments, etc.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, date

from database_ops import DatabaseManager, Account, AccountType
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

# Configure logging
logger = logging.getLogger(__name__)


class AccountManager:
    """
    Manages financial accounts (banks, credit cards, investments, etc.).
    
    Provides CRUD operations and balance calculations for accounts.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the account manager.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
        logger.info("Account manager initialized")
    
    def create_account(
        self,
        name: str,
        account_type: AccountType,
        initial_balance: float = 0.0
    ) -> Optional[Account]:
        """
        Create a new account.
        
        Args:
            name: Account name (must be unique)
            account_type: Account type (AccountType enum)
            initial_balance: Initial account balance
        
        Returns:
            Created Account object, or None if creation failed
        
        Raises:
            ValueError: If account name already exists
        """
        session = self.db_manager.get_session()
        
        try:
            # Check if account with same name exists
            existing = self.db_manager.get_account_by_name(name, session=session)
            if existing:
                raise ValueError(f"Account with name '{name}' already exists")
            
            # Create new account
            account = Account(
                name=name,
                type=account_type,
                balance=initial_balance
            )
            
            session.add(account)
            session.commit()
            session.refresh(account)  # Load all attributes before session closes
            
            # Access attributes while session is still open to avoid detached instance errors
            account_id = account.id
            account_name = account.name
            account_type_value = account.type.value
            account_balance = account.balance
            
            logger.info(f"Created account: {name} ({account_type.value}) with balance {initial_balance}")
            
            # Make object accessible after session closes
            session.expunge(account)
            return account
            
        except ValueError:
            session.rollback()
            raise
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to create account: {e}")
            return None
        finally:
            session.close()
    
    def get_account(self, account_id: int) -> Optional[Account]:
        """
        Get an account by ID.
        
        Args:
            account_id: Account ID
        
        Returns:
            Account object or None if not found
        """
        return self.db_manager.get_account(account_id)
    
    def get_account_by_name(self, name: str) -> Optional[Account]:
        """
        Get an account by name.
        
        Args:
            name: Account name
        
        Returns:
            Account object or None if not found
        """
        return self.db_manager.get_account_by_name(name)
    
    def list_accounts(self, account_type: Optional[AccountType] = None) -> List[Account]:
        """
        List all accounts, optionally filtered by type.
        
        Args:
            account_type: Optional filter by account type
        
        Returns:
            List of Account objects
        """
        accounts = self.db_manager.get_all_accounts()
        
        if account_type:
            accounts = [acc for acc in accounts if acc.type == account_type]
        
        return accounts
    
    def update_account(
        self,
        account_id: int,
        name: Optional[str] = None,
        account_type: Optional[AccountType] = None,
        balance: Optional[float] = None
    ) -> Optional[Account]:
        """
        Update an account.
        
        Args:
            account_id: Account ID
            name: New account name (optional)
            account_type: New account type (optional)
            balance: New balance (optional)
        
        Returns:
            Updated Account object, or None if update failed
        """
        session = self.db_manager.get_session()
        
        try:
            account = session.query(Account).filter(Account.id == account_id).first()
            if not account:
                logger.warning(f"Account {account_id} not found")
                return None
            
            if name is not None:
                # Check if new name conflicts with existing account
                existing = self.db_manager.get_account_by_name(name, session=session)
                if existing and existing.id != account_id:
                    raise ValueError(f"Account with name '{name}' already exists")
                account.name = name
            
            if account_type is not None:
                account.type = account_type
            
            if balance is not None:
                account.balance = balance
            
            account.updated_at = datetime.utcnow()
            session.commit()
            
            logger.info(f"Updated account {account_id}")
            return account
            
        except ValueError:
            session.rollback()
            raise
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to update account: {e}")
            return None
        finally:
            session.close()
    
    def delete_account(self, account_id: int) -> bool:
        """
        Delete an account.
        
        Note: This will not delete associated transactions, but will
        set their account_id to None.
        
        Args:
            account_id: Account ID
        
        Returns:
            True if deletion succeeded, False otherwise
        """
        session = self.db_manager.get_session()
        
        try:
            account = session.query(Account).filter(Account.id == account_id).first()
            if not account:
                logger.warning(f"Account {account_id} not found")
                return False
            
            # Check if account has transactions
            from database_ops import Transaction
            transaction_count = session.query(Transaction).filter(
                Transaction.account_id == account_id
            ).count()
            
            if transaction_count > 0:
                logger.warning(
                    f"Account {account_id} has {transaction_count} transactions. "
                    "Setting account_id to None for these transactions."
                )
                # Set account_id to None for all transactions
                session.query(Transaction).filter(
                    Transaction.account_id == account_id
                ).update({Transaction.account_id: None})
            
            session.delete(account)
            session.commit()
            
            logger.info(f"Deleted account {account_id}")
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to delete account: {e}")
            return False
        finally:
            session.close()
    
    def recalculate_balance(self, account_id: int) -> Optional[float]:
        """
        Recalculate account balance from transactions.
        
        Args:
            account_id: Account ID
        
        Returns:
            Calculated balance, or None if account not found
        """
        account = self.get_account(account_id)
        if not account:
            return None
        
        balance = self.db_manager.calculate_account_balance(account_id)
        
        # Update account balance
        self.update_account(account_id, balance=balance)
        
        logger.info(f"Recalculated balance for account {account_id}: {balance}")
        return balance
    
    def get_account_summary(self, account_id: int) -> Optional[Dict[str, Any]]:
        """
        Get summary information for an account.
        
        Args:
            account_id: Account ID
        
        Returns:
            Dictionary with account summary, or None if account not found
        """
        account = self.get_account(account_id)
        if not account:
            return None
        
        session = self.db_manager.get_session()
        try:
            from database_ops import Transaction
            
            # Get transaction statistics
            total_transactions = session.query(Transaction).filter(
                Transaction.account_id == account_id
            ).count()
            
            # Get balance from transactions
            calculated_balance = self.db_manager.calculate_account_balance(account_id, session=session)
            
            # Get recent transactions count (last 30 days)
            from datetime import timedelta
            thirty_days_ago = datetime.utcnow() - timedelta(days=30)
            recent_count = session.query(Transaction).filter(
                Transaction.account_id == account_id,
                Transaction.date >= thirty_days_ago
            ).count()
            
            summary = {
                "id": account.id,
                "name": account.name,
                "type": account.type.value,
                "stored_balance": account.balance,
                "calculated_balance": calculated_balance,
                "total_transactions": total_transactions,
                "recent_transactions": recent_count,
                "created_at": account.created_at,
                "updated_at": account.updated_at
            }
            
            return summary
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get account summary: {e}")
            return None
        finally:
            session.close()
    
    def update_balance(
        self,
        account_id: int,
        new_balance: float,
        notes: Optional[str] = None
    ) -> bool:
        """
        Manually update account balance and record in history.
        
        This is typically used for investment/savings accounts where
        the balance cannot be automatically calculated from transactions.
        
        Args:
            account_id: Account ID to update
            new_balance: New balance value
            notes: Optional notes about this balance update
        
        Returns:
            True if successful, False otherwise
        """
        from database_ops import BalanceHistory
        
        session = self.db_manager.get_session()
        
        try:
            # Get the account
            account = session.query(Account).filter(Account.id == account_id).first()
            if not account:
                logger.error(f"Account {account_id} not found")
                return False
            
            # Update account balance
            old_balance = account.balance
            account.balance = new_balance
            
            # Record in balance history
            history_entry = BalanceHistory(
                account_id=account_id,
                balance=new_balance,
                timestamp=datetime.utcnow(),
                notes=notes
            )
            session.add(history_entry)
            
            session.commit()
            
            logger.info(
                f"Updated balance for account {account_id} ({account.name}): "
                f"${old_balance:.2f} -> ${new_balance:.2f}"
            )
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to update balance: {e}")
            return False
        finally:
            session.close()
    
    def get_balance_history(
        self,
        account_id: int,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get balance history for an account.
        
        Args:
            account_id: Account ID
            limit: Optional limit on number of history entries
        
        Returns:
            List of balance history entries (dicts)
        """
        from database_ops import BalanceHistory
        
        session = self.db_manager.get_session()
        
        try:
            query = session.query(BalanceHistory).filter(
                BalanceHistory.account_id == account_id
            ).order_by(BalanceHistory.timestamp.desc())
            
            if limit:
                query = query.limit(limit)
            
            entries = query.all()
            
            history = []
            for entry in entries:
                history.append({
                    'id': entry.id,
                    'balance': entry.balance,
                    'timestamp': entry.timestamp,
                    'notes': entry.notes
                })
            
            logger.info(f"Retrieved {len(history)} balance history entries for account {account_id}")
            return history
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get balance history: {e}")
            return []
        finally:
            session.close()
    
    def get_or_create_account(
        self,
        name: str,
        account_type: AccountType,
        initial_balance: float = 0.0
    ) -> Optional[Account]:
        """
        Get existing account by name or create it if it doesn't exist.
        
        Args:
            name: Account name
            account_type: Account type
            initial_balance: Initial balance if creating new account
        
        Returns:
            Account object or None if error
        """
        # Try to get existing account
        account = self.get_account_by_name(name)
        
        if account:
            logger.info(f"Found existing account: {name}")
            return account
        
        # Create new account
        logger.info(f"Creating new account: {name} ({account_type.value})")
        return self.create_account(name, account_type, initial_balance)
    
    def set_balance_override(
        self,
        account_id: int,
        override_date: date,
        override_balance: float,
        notes: Optional[str] = None
    ) -> bool:
        """
        Set a balance override for an account as of a specific date.
        
        This is useful when you have incomplete historical transaction data
        but know the balance as of a certain date. The current balance will
        be calculated as: override_balance + sum(transactions after override_date).
        
        Args:
            account_id: Account ID
            override_date: Date for which the balance is known
            override_balance: Known balance as of override_date
            notes: Optional notes about this override
        
        Returns:
            True if successful, False otherwise
        """
        from database_ops import BalanceOverride
        
        session = self.db_manager.get_session()
        
        try:
            # Validate account exists
            account = session.query(Account).filter(Account.id == account_id).first()
            if not account:
                logger.error(f"Account {account_id} not found")
                return False
            
            # Validate date is not in the future
            if override_date > date.today():
                logger.warning(f"Override date {override_date} is in the future")
                # Allow but warn
            
            # Create override
            override = BalanceOverride(
                account_id=account_id,
                override_date=override_date,
                override_balance=override_balance,
                created_at=datetime.utcnow(),
                notes=notes
            )
            session.add(override)
            session.commit()
            
            logger.info(
                f"Set balance override for account {account_id} ({account.name}): "
                f"${override_balance:.2f} as of {override_date}"
            )
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to set balance override: {e}")
            return False
        finally:
            session.close()
    
    def get_balance_with_override(
        self,
        account_id: int,
        as_of_date: Optional[date] = None
    ) -> float:
        """
        Calculate account balance considering any balance overrides.
        
        Logic:
        1. Find the most recent override where override_date <= as_of_date
        2. If override found: sum transactions where date > override_date AND date <= as_of_date
        3. Return override_balance + transaction_sum
        4. If no override: sum all transactions where date <= as_of_date
        
        Args:
            account_id: Account ID
            as_of_date: Date to calculate balance as of (defaults to today)
        
        Returns:
            Calculated balance as a float
        """
        from database_ops import BalanceOverride, Transaction
        
        if as_of_date is None:
            as_of_date = date.today()
        
        session = self.db_manager.get_session()
        
        try:
            # Find the most recent override on or before as_of_date
            override = session.query(BalanceOverride).filter(
                BalanceOverride.account_id == account_id,
                BalanceOverride.override_date <= as_of_date
            ).order_by(BalanceOverride.override_date.desc()).first()
            
            if override:
                # Calculate balance from override
                logger.debug(
                    f"Using override for account {account_id}: "
                    f"${override.override_balance:.2f} as of {override.override_date}"
                )
                
                # Sum transactions after override date and up to as_of_date
                transaction_sum = session.query(func.sum(Transaction.amount)).filter(
                    Transaction.account_id == account_id,
                    Transaction.date > override.override_date,
                    Transaction.date <= as_of_date
                ).scalar() or 0.0
                
                balance = override.override_balance + transaction_sum
                
                logger.debug(
                    f"Balance calculation: override ${override.override_balance:.2f} + "
                    f"transactions ${transaction_sum:.2f} = ${balance:.2f}"
                )
            else:
                # No override, sum all transactions up to as_of_date
                logger.debug(f"No override found for account {account_id}, summing all transactions")
                
                transaction_sum = session.query(func.sum(Transaction.amount)).filter(
                    Transaction.account_id == account_id,
                    Transaction.date <= as_of_date
                ).scalar() or 0.0
                
                balance = transaction_sum
            
            return balance
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to calculate balance with override: {e}")
            return 0.0
        finally:
            session.close()
    
    def get_balance_overrides(
        self,
        account_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get all balance overrides for an account.
        
        Args:
            account_id: Account ID
        
        Returns:
            List of override dictionaries
        """
        from database_ops import BalanceOverride
        
        session = self.db_manager.get_session()
        
        try:
            overrides = session.query(BalanceOverride).filter(
                BalanceOverride.account_id == account_id
            ).order_by(BalanceOverride.override_date.desc()).all()
            
            result = []
            for override in overrides:
                result.append({
                    'id': override.id,
                    'override_date': override.override_date,
                    'override_balance': override.override_balance,
                    'created_at': override.created_at,
                    'notes': override.notes
                })
            
            logger.info(f"Retrieved {len(result)} overrides for account {account_id}")
            return result
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get balance overrides: {e}")
            return []
        finally:
            session.close()
    
    def delete_balance_override(
        self,
        override_id: int
    ) -> bool:
        """
        Delete a balance override.
        
        Args:
            override_id: Override ID to delete
        
        Returns:
            True if successful, False otherwise
        """
        from database_ops import BalanceOverride
        
        session = self.db_manager.get_session()
        
        try:
            override = session.query(BalanceOverride).filter(
                BalanceOverride.id == override_id
            ).first()
            
            if not override:
                logger.error(f"Override {override_id} not found")
                return False
            
            session.delete(override)
            session.commit()
            
            logger.info(f"Deleted balance override {override_id}")
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to delete balance override: {e}")
            return False
        finally:
            session.close()
    
    def get_signed_balance(
        self,
        account_id: int,
        as_of_date: Optional[date] = None
    ) -> float:
        """
        Get account balance with proper sign for asset/liability accounting.
        
        Credit accounts (debts) are returned as negative values (liabilities).
        All other account types are returned as positive values (assets).
        
        This is the correct way to calculate net worth:
        Net Worth = Assets - Liabilities = sum(all signed balances)
        
        Args:
            account_id: Account ID
            as_of_date: Date to calculate balance as of (defaults to today)
        
        Returns:
            Signed balance (negative for credit accounts)
        """
        # Get the unsigned balance (respecting overrides)
        balance = self.get_balance_with_override(account_id, as_of_date)
        
        # Get account type
        account = self.get_account(account_id)
        if not account:
            logger.error(f"Account {account_id} not found")
            return 0.0
        
        # Invert sign for credit accounts (they are liabilities/debts)
        if account.type == AccountType.CREDIT:
            return -abs(balance)
        
        return balance

