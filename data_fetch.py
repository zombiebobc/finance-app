"""
Data fetching module for analytics dashboard.

This module provides functions to fetch and process account data,
including historical balances, account summaries, and balance history.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import Session

from database_ops import DatabaseManager, Account, Transaction, AccountType
from account_management import AccountManager

logger = logging.getLogger(__name__)


def fetch_account_summaries(
    db_manager: DatabaseManager,
    as_of_date: Optional[date] = None
) -> Dict[str, pd.DataFrame]:
    """
    Fetch account summaries with assets/liabilities split.
    
    Args:
        db_manager: Database manager instance
        as_of_date: Date to calculate balances as of (defaults to today)
    
    Returns:
        Dictionary with 'assets', 'liabilities' DataFrames and totals
    """
    if as_of_date is None:
        as_of_date = date.today()
    
    account_manager = AccountManager(db_manager)
    accounts = account_manager.list_accounts()
    
    if not accounts:
        return {
            'assets': pd.DataFrame(columns=['id', 'name', 'type', 'balance']),
            'liabilities': pd.DataFrame(columns=['id', 'name', 'type', 'balance']),
            'net_worth': 0.0,
            'assets_total': 0.0,
            'liabilities_total': 0.0
        }
    
    # Get signed balances for all accounts as of specified date
    account_data = []
    for acc in accounts:
        signed_balance = account_manager.get_signed_balance(acc.id, as_of_date)
        account_data.append({
            'id': acc.id,
            'name': acc.name,
            'type': acc.type.value,
            'balance': signed_balance
        })
    
    # Create DataFrame
    df = pd.DataFrame(account_data)
    
    # Split into assets and liabilities
    assets_df = df[df['balance'] >= 0].copy()
    liabilities_df = df[df['balance'] < 0].copy()
    
    # Sort: assets descending, liabilities ascending (least negative first)
    assets_df = assets_df.sort_values('balance', ascending=False).reset_index(drop=True)
    liabilities_df = liabilities_df.sort_values('balance', ascending=False).reset_index(drop=True)
    
    # Calculate totals
    assets_total = assets_df['balance'].sum() if not assets_df.empty else 0.0
    liabilities_total = liabilities_df['balance'].sum() if not liabilities_df.empty else 0.0
    net_worth = assets_total + liabilities_total
    
    logger.info(
        f"Fetched account summaries as of {as_of_date}: "
        f"{len(assets_df)} assets, {len(liabilities_df)} liabilities, "
        f"net worth ${net_worth:,.2f}"
    )
    
    return {
        'assets': assets_df,
        'liabilities': liabilities_df,
        'net_worth': net_worth,
        'assets_total': assets_total,
        'liabilities_total': liabilities_total
    }


def calculate_historical_balance(
    db_manager: DatabaseManager,
    account_id: int,
    as_of: date
) -> float:
    """
    Calculate account balance as of a specific date using override-aware logic.
    
    Logic:
    1. Find most recent override where override_date <= as_of
    2. Sum transactions where transaction.date > override_date AND transaction.date <= as_of
    3. Return override_balance + transaction_sum
    
    Args:
        db_manager: Database manager instance
        account_id: Account ID
        as_of: Date to calculate balance as of
    
    Returns:
        Calculated balance as of the specified date
    """
    account_manager = AccountManager(db_manager)
    return account_manager.get_signed_balance(account_id, as_of)


def fetch_balance_history(
    db_manager: DatabaseManager,
    account_id: int,
    days: int = 30
) -> pd.DataFrame:
    """
    Fetch balance history for an account over the specified period.
    
    Creates a daily balance series by calculating balance for each day.
    
    Args:
        db_manager: Database manager instance
        account_id: Account ID
        days: Number of days of history to fetch
    
    Returns:
        DataFrame with columns: date, balance
    """
    account_manager = AccountManager(db_manager)
    session = db_manager.get_session()
    
    try:
        # Get account
        account = account_manager.get_account(account_id)
        if not account:
            logger.warning(f"Account {account_id} not found")
            return pd.DataFrame(columns=['date', 'balance'])
        
        # Get date range
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        # Check if there are any transactions in this period
        transaction_count = session.query(Transaction).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.date >= start_date,
                Transaction.date <= end_date
            )
        ).count()
        
        if transaction_count == 0:
            # No transactions in period, return current balance for all dates
            current_balance = account_manager.get_signed_balance(account_id, end_date)
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            return pd.DataFrame({
                'date': dates,
                'balance': [current_balance] * len(dates)
            })
        
        # Calculate balance for each day
        balance_data = []
        current_date = start_date
        
        while current_date <= end_date:
            balance = account_manager.get_signed_balance(account_id, current_date)
            balance_data.append({
                'date': current_date,
                'balance': balance
            })
            current_date += timedelta(days=1)
        
        df = pd.DataFrame(balance_data)
        logger.info(f"Fetched {len(df)} days of balance history for account {account_id}")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching balance history: {e}", exc_info=True)
        return pd.DataFrame(columns=['date', 'balance'])
    finally:
        session.close()


def fetch_net_worth_history(
    db_manager: DatabaseManager,
    days: int = 90
) -> pd.DataFrame:
    """
    Fetch net worth history (sum of all account balances) over time.
    
    Args:
        db_manager: Database manager instance
        days: Number of days of history to fetch
    
    Returns:
        DataFrame with columns: date, net_worth
    """
    account_manager = AccountManager(db_manager)
    accounts = account_manager.list_accounts()
    
    if not accounts:
        return pd.DataFrame(columns=['date', 'net_worth'])
    
    # Get date range
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    # Calculate net worth for each day
    net_worth_data = []
    current_date = start_date
    
    while current_date <= end_date:
        # Sum all account balances for this date
        daily_net_worth = 0.0
        for acc in accounts:
            balance = account_manager.get_signed_balance(acc.id, current_date)
            daily_net_worth += balance
        
        net_worth_data.append({
            'date': current_date,
            'net_worth': daily_net_worth
        })
        current_date += timedelta(days=1)
    
    df = pd.DataFrame(net_worth_data)
    logger.info(f"Fetched {len(df)} days of net worth history")
    return df


def get_time_frame_dates(time_frame: str) -> Tuple[date, date]:
    """
    Convert time frame string to date range.
    
    Args:
        time_frame: Time frame string ('Current', 'Last Month', 'Last Quarter', or 'YYYY-MM-DD')
    
    Returns:
        Tuple of (start_date, end_date)
    """
    today = date.today()
    
    if time_frame == 'Current':
        return today, today
    elif time_frame == 'Last Month':
        # Last month end
        first_of_month = today.replace(day=1)
        end_date = first_of_month - timedelta(days=1)
        start_date = end_date.replace(day=1)
        return start_date, end_date
    elif time_frame == 'Last Quarter':
        # Last 3 months
        end_date = today
        start_date = today - timedelta(days=90)
        return start_date, end_date
    else:
        # Try to parse as date
        try:
            custom_date = datetime.strptime(time_frame, '%Y-%m-%d').date()
            return custom_date, custom_date
        except ValueError:
            logger.warning(f"Invalid time frame: {time_frame}, defaulting to current")
            return today, today

