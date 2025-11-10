"""
Database utilities for account verification.

This module provides functions for connecting to the database and querying
transaction data with appropriate filters for date ranges and transaction types.
"""

import sqlite3
import logging
from datetime import date, datetime
from typing import Optional, Union
import pandas as pd


# Set up logging
logger = logging.getLogger(__name__)


def connect_to_database(db_path: str) -> sqlite3.Connection:
    """
    Connect to SQLite database.
    
    Args:
        db_path: Path to the SQLite database file.
        
    Returns:
        sqlite3.Connection: Active database connection.
        
    Raises:
        sqlite3.Error: If connection fails.
        FileNotFoundError: If database file doesn't exist.
        
    Example:
        >>> conn = connect_to_database("finances.db")
        >>> # Use connection for queries
        >>> conn.close()
    """
    try:
        logger.info(f"Attempting to connect to database: {db_path}")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        logger.info("Successfully connected to database")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to database: {e}")
        raise


def query_transactions(
    conn: sqlite3.Connection,
    start_date: date,
    end_date: date,
    exclude_transfers: bool = True,
    account_filter: Optional[str] = None
) -> pd.DataFrame:
    """
    Query transactions from database within specified date range.
    
    This function retrieves all transaction records within the given date range,
    optionally excluding transfers and filtering by account name. It handles
    date parsing and ensures proper filtering of transaction types.
    
    Args:
        conn: Active SQLite database connection.
        start_date: Start date for transaction query (inclusive).
        end_date: End date for transaction query (inclusive).
        exclude_transfers: If True, exclude transactions where type='transfer'.
        account_filter: Optional account name to filter by (exact match).
        
    Returns:
        pd.DataFrame: DataFrame containing transactions with columns:
            - account_name (str): Name of the account
            - date (datetime): Transaction date
            - amount (float): Transaction amount (positive=income, negative=expense)
            - category (str): Transaction category
            - type (str): Transaction type
            
    Raises:
        sqlite3.Error: If query execution fails.
        ValueError: If date range is invalid.
        
    Example:
        >>> conn = connect_to_database("finances.db")
        >>> start = date(2024, 11, 11)
        >>> end = date(2025, 11, 10)
        >>> df = query_transactions(conn, start, end, exclude_transfers=True)
        >>> print(df.head())
    """
    # Validate date range
    if start_date > end_date:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")
    
    logger.info(f"Querying transactions from {start_date} to {end_date}")
    logger.info(f"Exclude transfers: {exclude_transfers}")
    
    # Build SQL query with parameterized inputs for security
    query = """
        SELECT 
            account_name,
            date,
            amount,
            category,
            type
        FROM transactions
        WHERE date >= ? AND date <= ?
    """
    
    params = [start_date.isoformat(), end_date.isoformat()]
    
    # Add transfer exclusion filter
    if exclude_transfers:
        query += " AND LOWER(type) != 'transfer'"
        logger.debug("Added transfer exclusion to query")
    
    # Add account filter if provided
    if account_filter:
        query += " AND account_name = ?"
        params.append(account_filter)
        logger.debug(f"Added account filter: {account_filter}")
    
    query += " ORDER BY date, account_name"
    
    try:
        logger.debug(f"Executing query: {query}")
        logger.debug(f"Parameters: {params}")
        
        # Execute query and load into DataFrame
        df = pd.read_sql_query(query, conn, params=params)
        
        # Convert date column to datetime
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            logger.info(f"Retrieved {len(df)} transactions")
        else:
            logger.warning("Query returned no transactions")
        
        return df
        
    except sqlite3.Error as e:
        logger.error(f"Error executing query: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during query: {e}")
        raise


def verify_database_schema(conn: sqlite3.Connection) -> bool:
    """
    Verify that the database has the expected schema.
    
    Checks that the transactions table exists and has the required columns:
    account_name, date, amount, category, type.
    
    Args:
        conn: Active SQLite database connection.
        
    Returns:
        bool: True if schema is valid, False otherwise.
        
    Example:
        >>> conn = connect_to_database("finances.db")
        >>> if verify_database_schema(conn):
        ...     print("Schema is valid")
    """
    try:
        cursor = conn.cursor()
        
        # Check if transactions table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='transactions'
        """)
        
        if not cursor.fetchone():
            logger.error("Table 'transactions' does not exist")
            return False
        
        # Check for required columns
        cursor.execute("PRAGMA table_info(transactions)")
        columns = {row[1] for row in cursor.fetchall()}
        
        required_columns = {'account_name', 'date', 'amount', 'category', 'type'}
        missing_columns = required_columns - columns
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        logger.info("Database schema verification successful")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Error verifying schema: {e}")
        return False


def close_connection(conn: sqlite3.Connection) -> None:
    """
    Safely close database connection.
    
    Args:
        conn: SQLite database connection to close.
        
    Example:
        >>> conn = connect_to_database("finances.db")
        >>> # ... perform operations ...
        >>> close_connection(conn)
    """
    try:
        if conn:
            conn.close()
            logger.info("Database connection closed")
    except Exception as e:
        logger.warning(f"Error closing connection: {e}")

