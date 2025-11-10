"""
Database operations module for financial transaction storage.

This module handles database connections, schema creation, and data insertion
using SQLAlchemy ORM. Supports SQLite by default with easy migration to other databases.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Date, Index, ForeignKey, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.exc import SQLAlchemyError
import enum

# Configure logging
logger = logging.getLogger(__name__)

# Base class for declarative models
Base = declarative_base()


class AccountType(enum.Enum):
    """Enumeration of account types."""
    BANK = "bank"
    CREDIT = "credit"
    INVESTMENT = "investment"
    SAVINGS = "savings"
    CASH = "cash"
    OTHER = "other"


class Account(Base):
    """
    SQLAlchemy model representing a financial account.
    
    Attributes:
        id: Auto-incrementing primary key
        name: Account name (e.g., "Chase Checking")
        type: Account type (bank, credit, investment, etc.)
        balance: Current account balance
        created_at: Timestamp when account was created
        updated_at: Timestamp when account was last updated
    """
    
    __tablename__ = "accounts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    type = Column(Enum(AccountType), nullable=False, index=True)
    balance = Column(Float, default=0.0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationship to transactions
    transactions = relationship("Transaction", foreign_keys="[Transaction.account_id]", back_populates="account_ref")
    
    def __repr__(self) -> str:
        """String representation of the account."""
        return f"<Account(id={self.id}, name='{self.name}', type={self.type.value}, balance={self.balance})>"


class Budget(Base):
    """
    SQLAlchemy model representing a budget category.
    
    Attributes:
        id: Auto-incrementing primary key
        category: Category name
        allocated_amount: Amount allocated to this category
        period_start: Start date of budget period
        period_end: End date of budget period
        created_at: Timestamp when budget was created
        updated_at: Timestamp when budget was last updated
    """
    
    __tablename__ = "budgets"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(100), nullable=False, index=True)
    allocated_amount = Column(Float, nullable=False, default=0.0)
    period_start = Column(DateTime, nullable=False)
    period_end = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self) -> str:
        """String representation of the budget."""
        return (
            f"<Budget(id={self.id}, category='{self.category}', "
            f"allocated={self.allocated_amount}, period={self.period_start.date()} to {self.period_end.date()})>"
        )


class BalanceHistory(Base):
    """
    SQLAlchemy model representing historical balance snapshots.
    
    Tracks balance changes over time for investment and savings accounts.
    
    Attributes:
        id: Auto-incrementing primary key
        account_id: Foreign key to accounts table
        balance: Account balance at this point in time
        timestamp: When this balance was recorded
        notes: Optional notes about the balance update
    """
    
    __tablename__ = "balance_history"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)
    balance = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    notes = Column(String(255), nullable=True)
    
    # Relationship to account
    account = relationship("Account")
    
    def __repr__(self) -> str:
        """String representation of the balance history entry."""
        return (
            f"<BalanceHistory(id={self.id}, account_id={self.account_id}, "
            f"balance={self.balance}, timestamp={self.timestamp})>"
        )


class BalanceOverride(Base):
    """
    SQLAlchemy model representing balance overrides for accounts.
    
    Balance overrides allow users to set a known balance as of a specific date,
    which is useful when historical transaction data is incomplete. The current
    balance is then calculated as: override_balance + sum(transactions after override_date).
    
    Attributes:
        id: Auto-incrementing primary key
        account_id: Foreign key to accounts table
        override_date: Date for which the balance is known
        override_balance: Known balance as of override_date
        created_at: When this override was created
        notes: Optional notes about this override
    """
    
    __tablename__ = "balance_overrides"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)
    override_date = Column(Date, nullable=False, index=True)
    override_balance = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    notes = Column(String(255), nullable=True)
    
    # Relationship to account
    account = relationship("Account")
    
    # Composite index for efficient queries
    __table_args__ = (
        Index('idx_account_override_date', 'account_id', 'override_date'),
    )
    
    def __repr__(self) -> str:
        """String representation of the balance override."""
        return (
            f"<BalanceOverride(id={self.id}, account_id={self.account_id}, "
            f"date={self.override_date}, balance={self.override_balance})>"
        )


class Transaction(Base):
    """
    SQLAlchemy model representing a financial transaction.
    
    Attributes:
        id: Auto-incrementing primary key
        date: Transaction date (datetime)
        description: Transaction description
        amount: Transaction amount (float, 2 decimal places)
        category: Optional transaction category
        account: Optional account name
        source_file: Name of the CSV file from which this transaction was imported
        import_timestamp: Timestamp when this record was imported
        duplicate_hash: MD5 hash of key fields for duplicate detection
        is_transfer: Flag indicating if this is an internal transfer (1=yes, 0=no)
        transfer_to_account_id: Foreign key to destination account if this is a transfer
    """
    
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False, index=True)
    description = Column(String(500), nullable=False)
    amount = Column(Float, nullable=False)
    category = Column(String(100), nullable=True)
    account = Column(String(100), nullable=True)  # Legacy field for backward compatibility
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=True, index=True)
    source_file = Column(String(255), nullable=False)
    import_timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    duplicate_hash = Column(String(32), nullable=False, unique=True, index=True)
    is_transfer = Column(Integer, default=0, nullable=False)  # 0 = no, 1 = yes
    transfer_to_account_id = Column(Integer, ForeignKey("accounts.id"), nullable=True)
    
    # Relationship to account
    account_ref = relationship("Account", foreign_keys=[account_id], back_populates="transactions")
    transfer_to_account = relationship("Account", foreign_keys=[transfer_to_account_id])
    
    # Composite index for common queries
    __table_args__ = (
        Index('idx_date_amount', 'date', 'amount'),
        Index('idx_account_date', 'account_id', 'date'),
    )
    
    def __repr__(self) -> str:
        """String representation of the transaction."""
        return (
            f"<Transaction(id={self.id}, date={self.date}, "
            f"description='{self.description[:30]}...', amount={self.amount})>"
        )


class DatabaseManager:
    """
    Manages database connections and operations.
    
    This class handles database initialization, session management, and provides
    methods for inserting transactions and checking for duplicates.
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize the database manager.
        
        Args:
            connection_string: SQLAlchemy connection string (e.g., 'sqlite:///transactions.db')
        
        Raises:
            SQLAlchemyError: If database connection fails
        """
        try:
            self.engine = create_engine(connection_string, echo=False)
            self.SessionLocal = sessionmaker(bind=self.engine)
            logger.info(f"Database manager initialized with connection: {connection_string}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def create_tables(self) -> None:
        """
        Create all database tables if they don't exist.
        
        Raises:
            SQLAlchemyError: If table creation fails
        """
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created/verified successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database tables: {e}")
            raise
    
    def get_session(self) -> Session:
        """
        Get a new database session.
        
        Returns:
            SQLAlchemy session object
        
        Note:
            Caller is responsible for closing the session.
        """
        return self.SessionLocal()
    
    def check_duplicate_hashes(self, hashes: List[str], session: Optional[Session] = None) -> set:
        """
        Check which duplicate hashes already exist in the database.
        
        Args:
            hashes: List of duplicate hash strings to check
            session: Optional existing session (creates new one if None)
        
        Returns:
            Set of hashes that already exist in the database
        
        Raises:
            SQLAlchemyError: If database query fails
        """
        close_session = False
        if session is None:
            session = self.get_session()
            close_session = True
        
        try:
            existing_hashes = set(
                session.query(Transaction.duplicate_hash)
                .filter(Transaction.duplicate_hash.in_(hashes))
                .all()
            )
            # Extract hash strings from tuples
            existing_hashes = {h[0] for h in existing_hashes}
            logger.debug(f"Found {len(existing_hashes)} existing duplicate hashes out of {len(hashes)} checked")
            return existing_hashes
        except SQLAlchemyError as e:
            logger.error(f"Failed to check duplicate hashes: {e}")
            raise
        finally:
            if close_session:
                session.close()
    
    def insert_transactions(
        self,
        transactions: List[Dict[str, Any]],
        session: Optional[Session] = None
    ) -> tuple[int, int]:
        """
        Insert new transactions into the database.
        
        Args:
            transactions: List of transaction dictionaries with keys:
                - date: datetime object
                - description: string
                - amount: float
                - category: optional string
                - account: optional string
                - source_file: string
                - duplicate_hash: string (MD5 hash)
            session: Optional existing session (creates new one if None)
        
        Returns:
            Tuple of (inserted_count, skipped_count)
        
        Raises:
            SQLAlchemyError: If database insertion fails
        """
        close_session = False
        if session is None:
            session = self.get_session()
            close_session = True
        
        inserted_count = 0
        skipped_count = 0
        
        try:
            for trans_dict in transactions:
                try:
                    transaction = Transaction(
                        date=trans_dict["date"],
                        description=trans_dict["description"],
                        amount=trans_dict["amount"],
                        category=trans_dict.get("category"),
                        account=trans_dict.get("account"),
                        account_id=trans_dict.get("account_id"),
                        source_file=trans_dict["source_file"],
                        import_timestamp=datetime.utcnow(),
                        duplicate_hash=trans_dict["duplicate_hash"],
                        is_transfer=trans_dict.get("is_transfer", 0),
                        transfer_to_account_id=trans_dict.get("transfer_to_account_id")
                    )
                    session.add(transaction)
                    session.flush()  # Flush after each transaction to catch duplicates early
                    inserted_count += 1
                except KeyError as e:
                    session.rollback()  # Rollback this transaction
                    logger.warning(f"Missing required field in transaction: {e}")
                    skipped_count += 1
                except SQLAlchemyError as e:
                    session.rollback()  # Rollback this transaction
                    # Check if it's a duplicate error
                    if "UNIQUE constraint failed" in str(e) or "duplicate" in str(e).lower():
                        logger.debug(f"Skipping duplicate transaction: {trans_dict.get('description', 'unknown')}")
                        skipped_count += 1
                    else:
                        logger.warning(f"Failed to add transaction: {e}")
                        skipped_count += 1
                except Exception as e:
                    session.rollback()  # Rollback this transaction
                    logger.warning(f"Failed to add transaction: {e}")
                    skipped_count += 1
            
            session.commit()
            logger.info(f"Inserted {inserted_count} transactions, skipped {skipped_count}")
            return inserted_count, skipped_count
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to insert transactions: {e}")
            raise
        finally:
            if close_session:
                session.close()
    
    def get_transaction_count(self, session: Optional[Session] = None) -> int:
        """
        Get the total number of transactions in the database.
        
        Args:
            session: Optional existing session (creates new one if None)
        
        Returns:
            Total count of transactions
        """
        close_session = False
        if session is None:
            session = self.get_session()
            close_session = True
        
        try:
            count = session.query(Transaction).count()
            return count
        except SQLAlchemyError as e:
            logger.error(f"Failed to get transaction count: {e}")
            return 0
        finally:
            if close_session:
                session.close()
    
    def get_transactions(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: str = "date",
        order_desc: bool = True,
        session: Optional[Session] = None
    ) -> List[Transaction]:
        """
        Get transactions from the database with optional filters.
        
        Uses parameterized queries to prevent SQL injection.
        
        Args:
            filters: Dictionary of filter criteria:
                - date_start: datetime or date string (inclusive start)
                - date_end: datetime or date string (inclusive end)
                - amount_min: float (minimum amount, inclusive)
                - amount_max: float (maximum amount, inclusive)
                - description_keywords: str or list of str (search in description)
                - category: str (exact category match)
                - source_file: str (exact source file match)
            limit: Maximum number of records to return
            offset: Number of records to skip (for pagination)
            order_by: Column name to sort by (default: "date")
            order_desc: If True, sort descending; if False, ascending
            session: Optional existing session (creates new one if None)
        
        Returns:
            List of Transaction objects matching the criteria
        
        Raises:
            SQLAlchemyError: If database query fails
        """
        from sqlalchemy import and_, or_
        from datetime import datetime, date
        
        close_session = False
        if session is None:
            session = self.get_session()
            close_session = True
        
        try:
            # Start with base query
            query = session.query(Transaction)
            
            # Apply filters if provided
            if filters:
                conditions = []
                
                # Date range filters
                if "date_start" in filters and filters["date_start"]:
                    date_start = filters["date_start"]
                    if isinstance(date_start, str):
                        date_start = datetime.fromisoformat(date_start.replace("Z", "+00:00"))
                    elif isinstance(date_start, date) and not isinstance(date_start, datetime):
                        date_start = datetime.combine(date_start, datetime.min.time())
                    conditions.append(Transaction.date >= date_start)
                
                if "date_end" in filters and filters["date_end"]:
                    date_end = filters["date_end"]
                    if isinstance(date_end, str):
                        date_end = datetime.fromisoformat(date_end.replace("Z", "+00:00"))
                    elif isinstance(date_end, date) and not isinstance(date_end, datetime):
                        date_end = datetime.combine(date_end, datetime.max.time())
                    conditions.append(Transaction.date <= date_end)
                
                # Amount range filters
                if "amount_min" in filters and filters["amount_min"] is not None:
                    conditions.append(Transaction.amount >= float(filters["amount_min"]))
                
                if "amount_max" in filters and filters["amount_max"] is not None:
                    conditions.append(Transaction.amount <= float(filters["amount_max"]))
                
                # Description keyword search (case-insensitive)
                if "description_keywords" in filters and filters["description_keywords"]:
                    keywords = filters["description_keywords"]
                    if isinstance(keywords, str):
                        keywords = [keywords]
                    keyword_conditions = [
                        Transaction.description.ilike(f"%{keyword}%")
                        for keyword in keywords
                    ]
                    conditions.append(or_(*keyword_conditions))
                
                # Category filter (exact match, case-insensitive)
                if "category" in filters and filters["category"]:
                    conditions.append(Transaction.category.ilike(f"%{filters['category']}%"))
                
                # Source file filter (exact match, case-insensitive)
                if "source_file" in filters and filters["source_file"]:
                    conditions.append(Transaction.source_file.ilike(f"%{filters['source_file']}%"))
                
                # Account filters
                if "account_id" in filters and filters["account_id"] is not None:
                    conditions.append(Transaction.account_id == int(filters["account_id"]))
                
                if "account_name" in filters and filters["account_name"]:
                    # Join with Account table to filter by name
                    conditions.append(Account.name.ilike(f"%{filters['account_name']}%"))
                    query = query.join(Account, Transaction.account_id == Account.id)
                
                # Apply all conditions
                if conditions:
                    query = query.filter(and_(*conditions))
            
            # Apply ordering
            order_column = getattr(Transaction, order_by, Transaction.date)
            if order_desc:
                query = query.order_by(order_column.desc())
            else:
                query = query.order_by(order_column.asc())
            
            # Apply pagination
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            
            # Execute query
            transactions = query.all()
            logger.debug(f"Retrieved {len(transactions)} transactions with filters: {filters}")
            return transactions
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get transactions: {e}")
            raise
        finally:
            if close_session:
                session.close()
    
    def get_account(self, account_id: int, session: Optional[Session] = None) -> Optional[Account]:
        """
        Get an account by ID.
        
        Args:
            account_id: Account ID
            session: Optional existing session
        
        Returns:
            Account object or None if not found
        """
        close_session = False
        if session is None:
            session = self.get_session()
            close_session = True
        
        try:
            account = session.query(Account).filter(Account.id == account_id).first()
            return account
        except SQLAlchemyError as e:
            logger.error(f"Failed to get account: {e}")
            return None
        finally:
            if close_session:
                session.close()
    
    def get_account_by_name(self, name: str, session: Optional[Session] = None) -> Optional[Account]:
        """
        Get an account by name.
        
        Args:
            name: Account name
            session: Optional existing session
        
        Returns:
            Account object or None if not found
        """
        close_session = False
        if session is None:
            session = self.get_session()
            close_session = True
        
        try:
            account = session.query(Account).filter(Account.name == name).first()
            return account
        except SQLAlchemyError as e:
            logger.error(f"Failed to get account by name: {e}")
            return None
        finally:
            if close_session:
                session.close()
    
    def get_all_accounts(self, session: Optional[Session] = None) -> List[Account]:
        """
        Get all accounts.
        
        Args:
            session: Optional existing session
        
        Returns:
            List of Account objects
        """
        close_session = False
        if session is None:
            session = self.get_session()
            close_session = True
        
        try:
            accounts = session.query(Account).order_by(Account.name).all()
            return accounts
        except SQLAlchemyError as e:
            logger.error(f"Failed to get accounts: {e}")
            return []
        finally:
            if close_session:
                session.close()
    
    def calculate_account_balance(self, account_id: int, session: Optional[Session] = None) -> float:
        """
        Calculate account balance from transactions.
        
        Args:
            account_id: Account ID
            session: Optional existing session
        
        Returns:
            Calculated balance
        """
        close_session = False
        if session is None:
            session = self.get_session()
            close_session = True
        
        try:
            # Sum all non-transfer transactions for this account
            result = session.query(
                Transaction.amount
            ).filter(
                Transaction.account_id == account_id,
                Transaction.is_transfer == 0
            ).all()
            
            balance = sum(amount[0] for amount in result) if result else 0.0
            return balance
        except SQLAlchemyError as e:
            logger.error(f"Failed to calculate account balance: {e}")
            return 0.0
        finally:
            if close_session:
                session.close()
    
    def close(self) -> None:
        """Close the database engine connection."""
        if hasattr(self, 'engine'):
            self.engine.dispose()
            logger.info("Database connection closed")

