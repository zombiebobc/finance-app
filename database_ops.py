"""
Database operations module for financial transaction storage.

This module handles database connections, schema creation, and data insertion
using SQLAlchemy ORM. Supports SQLite by default with easy migration to other databases.
"""

import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    create_engine,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship, sessionmaker, validates, declarative_base
import enum

from encryption_utils import (
    DecryptionError,
    EncryptedNumeric,
    EncryptedString,
    attach_sqlalchemy_listeners,
    derive_search_token,
    encrypt_transaction_payload,
    get_encryption_manager,
    is_ciphertext,
    register_sqlite_functions,
)
from exceptions import DatabaseError

# Configure logging
logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    """
    Get current UTC datetime with timezone awareness.
    
    Replaces deprecated datetime.utcnow() with timezone-aware alternative.
    All timestamps in the database are stored in UTC.
    
    Returns:
        Current datetime in UTC with timezone info
    """
    return datetime.now(UTC)


def _ensure_account_security_columns(engine) -> None:
    """
    Ensure supporting columns (like account name index) exist and are populated.
    """
    if engine.dialect.name != "sqlite":
        return

    try:
        with engine.begin() as connection:
            table_exists = connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'")
            ).fetchone()
            if not table_exists:
                return
            column_rows = connection.execute(text("PRAGMA table_info(accounts)")).fetchall()
            column_names = {row[1] for row in column_rows}
            if "name_index" not in column_names:
                connection.execute(text("ALTER TABLE accounts ADD COLUMN name_index TEXT"))
            connection.execute(
                text("CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_name_index ON accounts(name_index)")
            )

            manager = get_encryption_manager()
            rows = connection.execute(
                text("SELECT id, name, name_index FROM accounts")
            ).fetchall()
            for row in rows:
                row_id = row[0]
                stored_name = row[1]
                existing_index = row[2]
                if existing_index:
                    continue
                if not stored_name:
                    continue
                if is_ciphertext(stored_name):
                    try:
                        plaintext = manager.decrypt_value(stored_name, str)
                    except DecryptionError:
                        plaintext = None
                else:
                    plaintext = stored_name
                if not plaintext:
                    continue
                token = derive_search_token(plaintext)
                connection.execute(
                    text("UPDATE accounts SET name_index = :token WHERE id = :id"),
                    {"token": token, "id": row_id},
                )
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Failed to ensure account security columns: %s", exc)
        raise


# Base class for declarative models (using SQLAlchemy 2.0+ pattern)
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
    name = Column(EncryptedString(256), nullable=False)
    name_index = Column(String(96), nullable=False, unique=True, index=True)
    type = Column(Enum(AccountType), nullable=False, index=True)
    balance = Column(EncryptedNumeric(), default=0.0, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relationship to transactions
    transactions = relationship("Transaction", foreign_keys="[Transaction.account_id]", back_populates="account_ref")
    
    def __repr__(self) -> str:
        """String representation of the account."""
        return f"<Account(id={self.id}, name='{self.name}', type={self.type.value}, balance={self.balance})>"

    @validates("name")
    def _update_name_index(self, key: str, value: str) -> str:
        """Update deterministic search token whenever the plaintext name changes."""
        token = derive_search_token(value)
        if not token:
            raise ValueError("Account name cannot be empty.")
        self.name_index = token
        return value


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
    category = Column(EncryptedString(100), nullable=False, index=True)
    allocated_amount = Column(EncryptedNumeric(), nullable=False, default=0.0)
    period_start = Column(DateTime(timezone=True), nullable=False)
    period_end = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)
    
    def __repr__(self) -> str:
        """String representation of the budget."""
        return (
            f"<Budget(id={self.id}, category='{self.category}', "
            f"allocated={self.allocated_amount}, period={self.period_start.date()} to {self.period_end.date()})>"
        )


class IncomeOverride(Base):
    """Optional monthly income override values."""
    
    __tablename__ = "income_overrides"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    period_start = Column(Date, nullable=False, unique=True, index=True)
    period_end = Column(Date, nullable=False)
    override_amount = Column(EncryptedNumeric(), nullable=False)
    notes = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)
    
    def __repr__(self) -> str:
        return (
            f"<IncomeOverride(id={self.id}, period={self.period_start} to {self.period_end}, "
            f"amount={self.override_amount})>"
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
    balance = Column(EncryptedNumeric(), nullable=False)
    timestamp = Column(DateTime(timezone=True), default=utc_now, nullable=False, index=True)
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
    override_balance = Column(EncryptedNumeric(), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)
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
    date = Column(DateTime(timezone=True), nullable=False, index=True)
    description = Column(EncryptedString(500), nullable=False)
    amount = Column(EncryptedNumeric(), nullable=False)
    category = Column(EncryptedString(100), nullable=True)
    account = Column(EncryptedString(100), nullable=True)  # Legacy field for backward compatibility
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=True, index=True)
    source_file = Column(EncryptedString(255), nullable=False)
    import_timestamp = Column(DateTime(timezone=True), default=utc_now, nullable=False)
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
        Index('idx_category_date', 'category', 'date'),  # For category filtering with date ranges
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
            connection_string: SQLAlchemy connection string (e.g., 'sqlite:///data/transactions.db')
        
        Raises:
            SQLAlchemyError: If database connection fails
        """
        try:
            self.engine = create_engine(connection_string, echo=False)
            attach_sqlalchemy_listeners(self.engine)
            self.SessionLocal = sessionmaker(bind=self.engine)
            _ensure_account_security_columns(self.engine)
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
                        import_timestamp=utc_now(),
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


def _configure_sqlite_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Configure sqlite connection defaults."""
    conn.row_factory = sqlite3.Row
    register_sqlite_functions(conn)
    return conn


def get_sqlite_connection(
    db_path: str | Path,
    *,
    read_only: bool = False,
    detect_types: bool = True,
    isolation_level: Optional[str] = None,
) -> sqlite3.Connection:
    """Create and return a SQLite connection with sane defaults."""
    if isinstance(db_path, Path):
        raw_path = db_path
    else:
        raw_path = Path(db_path)

    if str(raw_path) == ":memory:":
        conn = sqlite3.connect(
            ":memory:",
            detect_types=sqlite3.PARSE_DECLTYPES if detect_types else 0,
            isolation_level=isolation_level,
            check_same_thread=False,
        )
        logger.debug("Opened in-memory SQLite connection.")
        return _configure_sqlite_connection(conn)

    path = raw_path
    if read_only:
        uri = f"file:{path.as_posix()}?mode=ro"
        conn = sqlite3.connect(
            uri,
            uri=True,
            detect_types=sqlite3.PARSE_DECLTYPES if detect_types else 0,
            isolation_level=isolation_level,
            check_same_thread=False,
        )
    else:
        conn = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES if detect_types else 0,
            isolation_level=isolation_level,
            check_same_thread=False,
        )

    logger.debug("Opened SQLite connection to %s (read_only=%s)", path, read_only)
    return _configure_sqlite_connection(conn)


def init_sqlite_db(conn: sqlite3.Connection) -> None:
    """Initialize database tables and indexes for SQLite usage."""
    schema_statements: Tuple[Tuple[str, Tuple[Any, ...]], ...] = (
        (
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                name_index TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                balance TEXT NOT NULL DEFAULT '0.0',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            (),
        ),
        (
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                description TEXT NOT NULL,
                amount TEXT NOT NULL,
                category TEXT,
                account TEXT,
                account_id INTEGER,
                source_file TEXT NOT NULL,
                import_timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                duplicate_hash TEXT NOT NULL UNIQUE,
                is_transfer INTEGER NOT NULL DEFAULT 0,
                transfer_to_account_id INTEGER,
                FOREIGN KEY(account_id) REFERENCES accounts(id),
                FOREIGN KEY(transfer_to_account_id) REFERENCES accounts(id)
            )
            """,
            (),
        ),
        (
            """
            CREATE TABLE IF NOT EXISTS budgets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                allocated_amount TEXT NOT NULL DEFAULT '0.0',
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            (),
        ),
        (
            """
            CREATE TABLE IF NOT EXISTS income_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_start TEXT NOT NULL UNIQUE,
                period_end TEXT NOT NULL,
                override_amount TEXT NOT NULL,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            (),
        ),
        (
            """
            CREATE TABLE IF NOT EXISTS balance_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                override_date TEXT NOT NULL,
                override_balance TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
            """,
            (),
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_name_index ON accounts(name_index)",
            (),
        ),
        (
            # Fixed: Added index to improve transaction lookup by date.
            "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)",
            (),
        ),
        (
            # Fixed: Added index to improve transaction lookup by account/date.
            "CREATE INDEX IF NOT EXISTS idx_transactions_account_date ON transactions(account_id, date)",
            (),
        ),
        (
            # Fixed: Added index to accelerate duplicate hash lookups.
            "CREATE INDEX IF NOT EXISTS idx_transactions_duplicate_hash ON transactions(duplicate_hash)",
            (),
        ),
    )

    try:
        with conn:
            for statement, params in schema_statements:
                conn.execute(statement, params)
        logger.info("SQLite schema initialized successfully.")
    except sqlite3.Error as exc:
        logger.exception("Failed to initialize SQLite schema.")
        raise DatabaseError(
            "Failed to initialize database schema",
            details={"operation": "initialize_schema", "error": str(exc)},
            original_error=exc
        ) from exc


def insert_transaction_sqlite(conn: sqlite3.Connection, transaction_data: Dict[str, Any]) -> int:
    """Insert a single transaction using parameterized queries."""
    required_fields = ("date", "description", "amount", "source_file", "duplicate_hash")
    missing = [field for field in required_fields if field not in transaction_data]
    if missing:
        raise DatabaseError(
            "Missing required transaction fields",
            details={"missing_fields": missing, "required_fields": list(required_fields)}
        )

    try:
        payload = encrypt_transaction_payload(transaction_data)
        with conn:
            cursor = conn.execute(
                # Fixed: Parameterized query prevents SQL injection.
                """
                INSERT INTO transactions (
                    date,
                    description,
                    amount,
                    category,
                    account,
                    account_id,
                    source_file,
                    import_timestamp,
                    duplicate_hash,
                    is_transfer,
                    transfer_to_account_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?, ?, ?)
                """,
                (
                    transaction_data["date"],
                    payload.get("description"),
                    payload.get("amount"),
                    payload.get("category"),
                    payload.get("account"),
                    transaction_data.get("account_id"),
                    payload.get("source_file"),
                    transaction_data.get("import_timestamp"),
                    transaction_data["duplicate_hash"],
                    transaction_data.get("is_transfer", 0),
                    transaction_data.get("transfer_to_account_id"),
                ),
            )
        logger.debug("Inserted transaction with duplicate_hash=%s", transaction_data["duplicate_hash"])
        return int(cursor.lastrowid)
    except sqlite3.IntegrityError as exc:
        logger.warning("Integrity error inserting transaction: %s", exc)
        raise DatabaseError(
            "Transaction violates database constraints",
            details={"operation": "insert_transaction", "error": str(exc)},
            original_error=exc
        ) from exc
    except sqlite3.Error as exc:
        logger.exception("Unexpected SQLite error inserting transaction.")
        raise DatabaseError(
            "Failed to insert transaction",
            details={"operation": "insert_transaction", "error": str(exc)},
            original_error=exc
        ) from exc


def bulk_insert_transactions_sqlite(
    conn: sqlite3.Connection,
    transactions: Iterable[Dict[str, Any]],
) -> Tuple[int, int]:
    """Bulk insert transactions with parameterized executemany calls."""
    to_insert: List[Tuple[Any, ...]] = []
    skipped = 0

    for transaction in transactions:
        try:
            payload = encrypt_transaction_payload(transaction)
            required_tuple = (
                transaction["date"],
                payload.get("description"),
                payload.get("amount"),
                payload.get("category"),
                payload.get("account"),
                transaction.get("account_id"),
                payload.get("source_file"),
                transaction.get("import_timestamp"),
                transaction["duplicate_hash"],
                transaction.get("is_transfer", 0),
                transaction.get("transfer_to_account_id"),
            )
            to_insert.append(required_tuple)
        except KeyError as exc:
            skipped += 1
            logger.warning("Skipping transaction missing field %s", exc)

    if not to_insert:
        return 0, skipped

    try:
        with conn:
            conn.executemany(
                # Fixed: Parameterized query across executemany prevents SQL injection.
                """
                INSERT INTO transactions (
                    date,
                    description,
                    amount,
                    category,
                    account,
                    account_id,
                    source_file,
                    import_timestamp,
                    duplicate_hash,
                    is_transfer,
                    transfer_to_account_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?, ?, ?)
                """,
                to_insert,
            )
        logger.info("Bulk inserted %d transactions (skipped=%d).", len(to_insert), skipped)
        return len(to_insert), skipped
    except sqlite3.IntegrityError as exc:
        logger.warning("Integrity error bulk inserting transactions: %s", exc)
        raise DatabaseError(
            "One or more transactions violate database constraints",
            details={"operation": "bulk_insert", "error": str(exc)},
            original_error=exc
        ) from exc
    except sqlite3.Error as exc:
        logger.exception("Unexpected SQLite error during bulk insert.")
        raise DatabaseError(
            "Failed to bulk insert transactions",
            details={"operation": "bulk_insert", "error": str(exc)},
            original_error=exc
        ) from exc


def query_transactions_sqlite(
    conn: sqlite3.Connection,
    filters: Optional[Dict[str, Any]] = None,
    *,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: str = "date",
    order_desc: bool = True,
) -> List[Dict[str, Any]]:
    """Query transactions with parameterized filters."""
    valid_order_columns = {
        "date",
        "amount",
        "description",
        "category",
        "account_id",
        "import_timestamp",
    }
    if order_by not in valid_order_columns:
        logger.debug("Invalid order_by '%s'; defaulting to 'date'.", order_by)
        order_by = "date"

    base_query = [
        """
        SELECT
            id,
            date,
            decrypt_text(description) AS description,
            decrypt_numeric(amount) AS amount,
            decrypt_text(category) AS category,
            decrypt_text(account) AS account,
            account_id,
            decrypt_text(source_file) AS source_file,
            import_timestamp,
            duplicate_hash,
            is_transfer,
            transfer_to_account_id
        FROM transactions
        """
    ]

    params: List[Any] = []
    conditions: List[str] = []
    filters = filters or {}

    if filters.get("date_start"):
        conditions.append("date >= ?")
        params.append(filters["date_start"])
    if filters.get("date_end"):
        conditions.append("date <= ?")
        params.append(filters["date_end"])
    if filters.get("amount_min") is not None:
        conditions.append("decrypt_numeric(amount) >= ?")
        params.append(filters["amount_min"])
    if filters.get("amount_max") is not None:
        conditions.append("decrypt_numeric(amount) <= ?")
        params.append(filters["amount_max"])
    if filters.get("category"):
        conditions.append("LOWER(decrypt_text(category)) LIKE LOWER(?)")
        params.append(f"%{filters['category']}%")
    if filters.get("description_keywords"):
        keywords = filters["description_keywords"]
        if isinstance(keywords, str):
            keywords = [keywords]
        keyword_conditions = ["LOWER(decrypt_text(description)) LIKE LOWER(?)" for _ in keywords]
        conditions.append(f"({' OR '.join(keyword_conditions)})")
        params.extend([f"%{kw}%" for kw in keywords])
    if filters.get("source_file"):
        conditions.append("LOWER(decrypt_text(source_file)) LIKE LOWER(?)")
        params.append(f"%{filters['source_file']}%")
    if filters.get("account_id") is not None:
        conditions.append("account_id = ?")
        params.append(filters["account_id"])
    if filters.get("is_transfer") is not None:
        conditions.append("is_transfer = ?")
        params.append(1 if filters["is_transfer"] else 0)

    if conditions:
        base_query.append("WHERE " + " AND ".join(conditions))

    direction = "DESC" if order_desc else "ASC"
    base_query.append(f"ORDER BY {order_by} {direction}")

    if limit is not None:
        base_query.append("LIMIT ?")
        params.append(limit)
        if offset is not None:
            base_query.append("OFFSET ?")
            params.append(offset)
    elif offset is not None:
        base_query.append("LIMIT -1 OFFSET ?")
        params.append(offset)

    final_query = "\n".join(base_query)

    try:
        cursor = conn.execute(
            # Fixed: Parameterized query for transaction lookup.
            final_query,
            params,
        )
        rows = cursor.fetchall()
        logger.debug("Retrieved %d rows via SQLite query.", len(rows))
        return [dict(row) for row in rows]
    except sqlite3.Error as exc:
        logger.exception("Failed to query transactions via SQLite.")
        raise DatabaseError(
            "Failed to retrieve transactions",
            details={"operation": "query_transactions", "error": str(exc)},
            original_error=exc
        ) from exc


def delete_transaction_sqlite(conn: sqlite3.Connection, transaction_id: int) -> None:
    """Delete a transaction safely using parameterized queries."""
    try:
        with conn:
            conn.execute(
                # Fixed: Parameterized query for safe deletion.
                "DELETE FROM transactions WHERE id = ?",
                (transaction_id,),
            )
        logger.debug("Deleted transaction id=%s", transaction_id)
    except sqlite3.Error as exc:
        logger.exception("Failed to delete transaction id=%s", transaction_id)
        raise DatabaseError(
            "Failed to delete transaction",
            details={"operation": "delete_transaction", "transaction_id": transaction_id, "error": str(exc)},
            original_error=exc
        ) from exc


def get_transaction_count_sqlite(conn: sqlite3.Connection) -> int:
    """Return the count of transactions using a parameterized query."""
    try:
        cursor = conn.execute(
            # Fixed: Parameterized query to guard against injection.
            "SELECT COUNT(*) AS total FROM transactions",
            (),
        )
        row = cursor.fetchone()
        total = int(row["total"]) if row else 0
        logger.debug("Transaction count via SQLite: %d", total)
        return total
    except sqlite3.Error as exc:
        logger.exception("Failed to count transactions via SQLite.")
        raise DatabaseError(
            "Failed to count transactions",
            details={"operation": "count_transactions", "error": str(exc)},
            original_error=exc
        ) from exc


