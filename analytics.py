"""
Analytics module for financial transaction analysis.

This module provides core data aggregation functions for analyzing
financial transactions, including category breakdowns, income/expense
summaries, and time-based trend analysis.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, extract

from database_ops import DatabaseManager, Transaction, Account, AccountType

logger = logging.getLogger(__name__)


class AnalyticsEngine:
    """
    Core analytics engine for financial data analysis.
    
    Provides aggregation, filtering, and analysis functions that are
    UI-agnostic and can be used in CLI, web UI, or API contexts.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the analytics engine.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        logger.info("Analytics engine initialized")
    
    def parse_time_frame(self, time_frame: str) -> Tuple[datetime, datetime]:
        """
        Parse time frame string into start and end dates.
        
        Supports formats:
        - '1m', '3m', '6m', '12m' (months from now)
        - 'YYYY-MM-DD:YYYY-MM-DD' (custom date range)
        - 'all' (all time)
        
        Args:
            time_frame: Time frame string
        
        Returns:
            Tuple of (start_date, end_date)
        
        Raises:
            ValueError: If time frame format is invalid
        """
        now = datetime.now()
        
        # Handle 'all' time
        if time_frame.lower() == 'all':
            return datetime(1900, 1, 1), now
        
        # Handle custom date range
        if ':' in time_frame:
            try:
                start_str, end_str = time_frame.split(':')
                start_date = datetime.strptime(start_str, '%Y-%m-%d')
                end_date = datetime.strptime(end_str, '%Y-%m-%d')
                return start_date, end_date
            except ValueError as e:
                raise ValueError(f"Invalid date range format. Use YYYY-MM-DD:YYYY-MM-DD: {e}")
        
        # Handle relative months (1m, 3m, 6m, 12m)
        if time_frame.endswith('m'):
            try:
                months = int(time_frame[:-1])
                start_date = now - timedelta(days=months * 30)  # Approximate
                return start_date, now
            except ValueError:
                raise ValueError(f"Invalid month format: {time_frame}")
        
        raise ValueError(f"Invalid time frame format: {time_frame}. Use '1m', '3m', '6m', '12m', 'all', or 'YYYY-MM-DD:YYYY-MM-DD'")
    
    def get_income_expense_summary(
        self,
        time_frame: str = 'all',
        account_id: Optional[int] = None,
        account_type: Optional[AccountType] = None
    ) -> Dict[str, Union[float, int]]:
        """
        Get summary of income, expenses, and net change.
        
        Args:
            time_frame: Time frame for analysis
            account_id: Optional account ID filter
            account_type: Optional account type filter
        
        Returns:
            Dictionary with income, expense, net, transaction counts
        """
        start_date, end_date = self.parse_time_frame(time_frame)
        session = self.db_manager.get_session()
        
        try:
            # Build base query
            query = session.query(Transaction).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date
                )
            )
            
            # Apply account filters
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            elif account_type:
                query = query.join(Account).filter(Account.type == account_type)
            
            # Get all transactions
            transactions = query.all()
            
            if not transactions:
                return {
                    'total_income': 0.0,
                    'total_expenses': 0.0,
                    'net_change': 0.0,
                    'income_count': 0,
                    'expense_count': 0,
                    'total_count': 0
                }
            
            # Separate income and expenses
            income = sum(t.amount for t in transactions if t.amount > 0)
            expenses = sum(abs(t.amount) for t in transactions if t.amount < 0)
            income_count = sum(1 for t in transactions if t.amount > 0)
            expense_count = sum(1 for t in transactions if t.amount < 0)
            
            return {
                'total_income': income,
                'total_expenses': expenses,
                'net_change': income - expenses,
                'income_count': income_count,
                'expense_count': expense_count,
                'total_count': len(transactions)
            }
        
        except Exception as e:
            logger.error(f"Failed to get income/expense summary: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def get_category_breakdown(
        self,
        time_frame: str = 'all',
        account_id: Optional[int] = None,
        account_type: Optional[AccountType] = None,
        expense_only: bool = True,
        include_transfers: bool = False
    ) -> pd.DataFrame:
        """
        Get spending breakdown by category.
        
        Args:
            time_frame: Time frame for analysis
            account_id: Optional account ID filter
            account_type: Optional account type filter
            expense_only: If True, only include expenses (negative amounts)
            include_transfers: If True, include internal transfers in breakdown
        
        Returns:
            DataFrame with columns: category, total, count, percentage
        """
        start_date, end_date = self.parse_time_frame(time_frame)
        session = self.db_manager.get_session()
        
        try:
            # Build query
            query = session.query(
                func.coalesce(Transaction.category, 'Uncategorized').label('category'),
                func.sum(Transaction.amount).label('total'),
                func.count(Transaction.id).label('count')
            ).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date
                )
            )
            
            # Filter by amount sign if expense_only
            if expense_only:
                query = query.filter(Transaction.amount < 0)
            
            # Exclude transfers unless explicitly included
            if not include_transfers:
                query = query.filter(Transaction.is_transfer == 0)
            
            # Apply account filters
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            elif account_type:
                query = query.join(Account).filter(Account.type == account_type)
            
            # Group by category
            query = query.group_by('category')
            
            # Execute query
            results = query.all()
            
            if not results:
                return pd.DataFrame(columns=['category', 'total', 'count', 'percentage'])
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=['category', 'total', 'count'])
            
            # Calculate absolute values for expenses
            if expense_only:
                df['total'] = df['total'].abs()
            
            # Calculate percentages
            total_sum = df['total'].sum()
            df['percentage'] = (df['total'] / total_sum * 100) if total_sum > 0 else 0
            
            # Sort by total descending
            df = df.sort_values('total', ascending=False).reset_index(drop=True)
            
            logger.info(f"Generated category breakdown with {len(df)} categories")
            return df
        
        except Exception as e:
            logger.error(f"Failed to get category breakdown: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def get_income_breakdown(
        self,
        time_frame: str = 'all',
        account_id: Optional[int] = None,
        account_type: Optional[AccountType] = None,
        include_transfers: bool = False
    ) -> pd.DataFrame:
        """
        Get income breakdown by category (positive amounts only).
        
        Args:
            time_frame: Time frame for analysis
            account_id: Optional account ID filter
            account_type: Optional account type filter
            include_transfers: If True, include internal transfers in breakdown
        
        Returns:
            DataFrame with columns: category, total, count, percentage
        """
        start_date, end_date = self.parse_time_frame(time_frame)
        session = self.db_manager.get_session()
        
        try:
            # Build query for income (positive amounts)
            query = session.query(
                func.coalesce(Transaction.category, 'Uncategorized').label('category'),
                func.sum(Transaction.amount).label('total'),
                func.count(Transaction.id).label('count')
            ).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date,
                    Transaction.amount > 0  # Only positive amounts (income)
                )
            )
            
            # Exclude transfers unless explicitly included
            if not include_transfers:
                query = query.filter(Transaction.is_transfer == 0)
            
            # Apply account filters
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            elif account_type:
                query = query.join(Account).filter(Account.type == account_type)
            
            # Group by category
            query = query.group_by('category')
            
            # Execute query
            results = query.all()
            
            if not results:
                return pd.DataFrame(columns=['category', 'total', 'count', 'percentage'])
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=['category', 'total', 'count'])
            
            # Calculate percentages
            total_sum = df['total'].sum()
            df['percentage'] = (df['total'] / total_sum * 100) if total_sum > 0 else 0
            
            # Sort by total descending
            df = df.sort_values('total', ascending=False).reset_index(drop=True)
            
            logger.info(f"Generated income breakdown with {len(df)} categories")
            return df
        
        except Exception as e:
            logger.error(f"Failed to get income breakdown: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def get_monthly_trends(
        self,
        time_frame: str = '12m',
        account_id: Optional[int] = None,
        account_type: Optional[AccountType] = None
    ) -> pd.DataFrame:
        """
        Get monthly income and expense trends.
        
        Args:
            time_frame: Time frame for analysis
            account_id: Optional account ID filter
            account_type: Optional account type filter
        
        Returns:
            DataFrame with columns: year, month, income, expenses, net
        """
        start_date, end_date = self.parse_time_frame(time_frame)
        session = self.db_manager.get_session()
        
        try:
            # Build query
            query = session.query(
                extract('year', Transaction.date).label('year'),
                extract('month', Transaction.date).label('month'),
                Transaction.amount
            ).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date
                )
            )
            
            # Exclude transfers from trends
            query = query.filter(Transaction.is_transfer == 0)
            
            # Apply account filters
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            elif account_type:
                query = query.join(Account).filter(Account.type == account_type)
            
            # Execute query and convert to DataFrame
            results = query.all()
            
            if not results:
                return pd.DataFrame(columns=['year', 'month', 'income', 'expenses', 'net', 'period'])
            
            df = pd.DataFrame(results, columns=['year', 'month', 'amount'])
            
            # Aggregate by year/month
            grouped = df.groupby(['year', 'month'])
            
            monthly_data = []
            for (year, month), group in grouped:
                income = group[group['amount'] > 0]['amount'].sum()
                expenses = group[group['amount'] < 0]['amount'].abs().sum()
                net = income - expenses
                
                monthly_data.append({
                    'year': int(year),
                    'month': int(month),
                    'income': income,
                    'expenses': expenses,
                    'net': net,
                    'period': f"{int(year)}-{int(month):02d}"
                })
            
            result_df = pd.DataFrame(monthly_data)
            result_df = result_df.sort_values(['year', 'month']).reset_index(drop=True)
            
            logger.info(f"Generated monthly trends with {len(result_df)} months")
            return result_df
        
        except Exception as e:
            logger.error(f"Failed to get monthly trends: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def get_comparison_data(
        self,
        current_df: pd.DataFrame,
        comparison_type: str,
        account_id: Optional[int] = None,
        account_type: Optional[AccountType] = None
    ) -> pd.DataFrame:
        """
        Get comparison data for a previous period based on current monthly trends.
        
        Args:
            current_df: DataFrame with current period monthly trends (must have 'year', 'month', 'period' columns)
            comparison_type: Type of comparison ('previous_month' or 'previous_year')
            account_id: Optional account ID filter (must match current_df filter)
            account_type: Optional account type filter (must match current_df filter)
        
        Returns:
            DataFrame with comparison period data in same format as current_df, or empty DataFrame if no data
        
        Raises:
            ValueError: If comparison_type is invalid or current_df is empty
        """
        if current_df.empty:
            logger.warning("Cannot get comparison data: current_df is empty")
            return pd.DataFrame(columns=['year', 'month', 'income', 'expenses', 'net', 'period'])
        
        if comparison_type not in ['previous_month', 'previous_year']:
            raise ValueError(f"Invalid comparison_type: {comparison_type}. Must be 'previous_month' or 'previous_year'")
        
        session = self.db_manager.get_session()
        
        try:
            # Determine comparison period dates
            # Get the date range from current_df
            min_period = current_df['period'].min()
            max_period = current_df['period'].max()
            
            # Parse period strings (format: YYYY-MM)
            min_year, min_month = map(int, min_period.split('-'))
            max_year, max_month = map(int, max_period.split('-'))
            
            if comparison_type == 'previous_month':
                # Shift all months back by 1
                comparison_periods = []
                for _, row in current_df.iterrows():
                    year = row['year']
                    month = row['month']
                    # Calculate previous month
                    if month == 1:
                        prev_year = year - 1
                        prev_month = 12
                    else:
                        prev_year = year
                        prev_month = month - 1
                    comparison_periods.append((prev_year, prev_month))
                
                # Get date range for comparison
                start_year, start_month = comparison_periods[0]
                end_year, end_month = comparison_periods[-1]
                
            elif comparison_type == 'previous_year':
                # Shift all years back by 1, keep same months
                comparison_periods = []
                for _, row in current_df.iterrows():
                    prev_year = row['year'] - 1
                    prev_month = row['month']
                    comparison_periods.append((prev_year, prev_month))
                
                # Get date range for comparison
                start_year, start_month = comparison_periods[0]
                end_year, end_month = comparison_periods[-1]
            
            # Calculate date range (first day of first month to last day of last month)
            start_date = datetime(start_year, start_month, 1)
            # Last day of end month
            if end_month == 12:
                end_date = datetime(end_year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(end_year, end_month + 1, 1) - timedelta(days=1)
            
            # Build query (same logic as get_monthly_trends)
            query = session.query(
                extract('year', Transaction.date).label('year'),
                extract('month', Transaction.date).label('month'),
                Transaction.amount
            ).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date
                )
            )
            
            # Exclude transfers
            query = query.filter(Transaction.is_transfer == 0)
            
            # Apply account filters (must match current_df filters)
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            elif account_type:
                query = query.join(Account).filter(Account.type == account_type)
            
            # Execute query
            results = query.all()
            
            if not results:
                logger.info(f"No comparison data found for {comparison_type}")
                return pd.DataFrame(columns=['year', 'month', 'income', 'expenses', 'net', 'period'])
            
            df = pd.DataFrame(results, columns=['year', 'month', 'amount'])
            
            # Aggregate by year/month
            grouped = df.groupby(['year', 'month'])
            
            monthly_data = []
            for (year, month), group in grouped:
                income = group[group['amount'] > 0]['amount'].sum()
                expenses = group[group['amount'] < 0]['amount'].abs().sum()
                net = income - expenses
                
                monthly_data.append({
                    'year': int(year),
                    'month': int(month),
                    'income': income,
                    'expenses': expenses,
                    'net': net,
                    'period': f"{int(year)}-{int(month):02d}"
                })
            
            result_df = pd.DataFrame(monthly_data)
            result_df = result_df.sort_values(['year', 'month']).reset_index(drop=True)
            
            logger.info(f"Generated comparison data with {len(result_df)} months for {comparison_type}")
            return result_df
        
        except Exception as e:
            logger.error(f"Failed to get comparison data: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def calculate_percentage_changes(
        self,
        current_df: pd.DataFrame,
        comparison_df: pd.DataFrame
    ) -> Dict[str, Dict[str, float]]:
        """
        Calculate percentage changes between current and comparison periods.
        
        Args:
            current_df: DataFrame with current period data
            comparison_df: DataFrame with comparison period data
        
        Returns:
            Dictionary with 'income', 'expenses', 'net' keys, each containing:
            - 'current': Total for current period
            - 'comparison': Total for comparison period
            - 'change': Absolute change
            - 'percent_change': Percentage change
        """
        if current_df.empty or comparison_df.empty:
            return {
                'income': {'current': 0.0, 'comparison': 0.0, 'change': 0.0, 'percent_change': 0.0},
                'expenses': {'current': 0.0, 'comparison': 0.0, 'change': 0.0, 'percent_change': 0.0},
                'net': {'current': 0.0, 'comparison': 0.0, 'change': 0.0, 'percent_change': 0.0}
            }
        
        current_income = current_df['income'].sum()
        current_expenses = current_df['expenses'].sum()
        current_net = current_df['net'].sum()
        
        comp_income = comparison_df['income'].sum()
        comp_expenses = comparison_df['expenses'].sum()
        comp_net = comparison_df['net'].sum()
        
        def calc_percent_change(current: float, comparison: float) -> float:
            """Calculate percentage change, handling zero comparison values."""
            if comparison == 0:
                return 100.0 if current > 0 else (0.0 if current == 0 else -100.0)
            return ((current - comparison) / abs(comparison)) * 100.0
        
        return {
            'income': {
                'current': current_income,
                'comparison': comp_income,
                'change': current_income - comp_income,
                'percent_change': calc_percent_change(current_income, comp_income)
            },
            'expenses': {
                'current': current_expenses,
                'comparison': comp_expenses,
                'change': current_expenses - comp_expenses,
                'percent_change': calc_percent_change(current_expenses, comp_expenses)
            },
            'net': {
                'current': current_net,
                'comparison': comp_net,
                'change': current_net - comp_net,
                'percent_change': calc_percent_change(current_net, comp_net)
            }
        }
    
    def get_account_summary(
        self,
        time_frame: str = 'all'
    ) -> pd.DataFrame:
        """
        Get spending summary by account.
        
        Args:
            time_frame: Time frame for analysis
        
        Returns:
            DataFrame with columns: account_name, type, income, expenses, net, count
        """
        start_date, end_date = self.parse_time_frame(time_frame)
        session = self.db_manager.get_session()
        
        try:
            # Query transactions with account info
            query = session.query(
                Account.name,
                Account.type,
                Transaction.amount
            ).join(
                Transaction, Transaction.account_id == Account.id
            ).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date
                )
            )
            
            results = query.all()
            
            if not results:
                return pd.DataFrame(columns=['account_name', 'type', 'income', 'expenses', 'net', 'count'])
            
            df = pd.DataFrame(results, columns=['account_name', 'type', 'amount'])
            
            # Convert AccountType enum to string for grouping
            df['type'] = df['type'].apply(lambda x: x.value if hasattr(x, 'value') else str(x))
            
            # Aggregate by account
            grouped = df.groupby(['account_name', 'type'])
            
            account_data = []
            for (account_name, account_type), group in grouped:
                income = group[group['amount'] > 0]['amount'].sum()
                expenses = group[group['amount'] < 0]['amount'].abs().sum()
                net = income - expenses
                count = len(group)
                
                account_data.append({
                    'account_name': account_name,
                    'type': account_type,  # Already converted to string above
                    'income': income,
                    'expenses': expenses,
                    'net': net,
                    'count': count
                })
            
            result_df = pd.DataFrame(account_data)
            result_df = result_df.sort_values('expenses', ascending=False).reset_index(drop=True)
            
            logger.info(f"Generated account summary with {len(result_df)} accounts")
            return result_df
        
        except Exception as e:
            logger.error(f"Failed to get account summary: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def get_account_summary_refined(
        self,
        as_of_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get refined account summary with Assets/Liabilities grouping.
        
        Returns accounts grouped into Assets (positive balances) and Liabilities
        (negative balances, e.g., credit cards), with signed balances where
        credit accounts show as negative values (debts).
        
        Args:
            as_of_date: Optional date string (YYYY-MM-DD) to calculate balances as of
        
        Returns:
            Dictionary with keys:
            - 'assets': DataFrame with asset accounts (balance >= 0)
            - 'liabilities': DataFrame with liability accounts (balance < 0)
            - 'net_worth': Float total net worth
            - 'assets_total': Float total assets
            - 'liabilities_total': Float total liabilities
        """
        from account_management import AccountManager
        from datetime import datetime, date
        
        # Parse as_of_date if provided
        query_date = None
        if as_of_date:
            try:
                query_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"Invalid date format: {as_of_date}, using today")
                query_date = date.today()
        else:
            query_date = date.today()
        
        account_manager = AccountManager(self.db_manager)
        accounts = account_manager.list_accounts()
        
        if not accounts:
            return {
                'assets': pd.DataFrame(columns=['id', 'name', 'type', 'balance']),
                'liabilities': pd.DataFrame(columns=['id', 'name', 'type', 'balance']),
                'net_worth': 0.0,
                'assets_total': 0.0,
                'liabilities_total': 0.0
            }
        
        # Get signed balances for all accounts
        account_data = []
        for acc in accounts:
            signed_balance = account_manager.get_signed_balance(acc.id, query_date)
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
            f"Generated refined account summary: {len(assets_df)} assets, "
            f"{len(liabilities_df)} liabilities, net worth ${net_worth:,.2f}"
        )
        
        return {
            'assets': assets_df,
            'liabilities': liabilities_df,
            'net_worth': net_worth,
            'assets_total': assets_total,
            'liabilities_total': liabilities_total
        }
    
    def get_top_transactions(
        self,
        time_frame: str = 'all',
        limit: int = 10,
        transaction_type: str = 'expenses',
        account_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get top transactions by amount.
        
        Args:
            time_frame: Time frame for analysis
            limit: Number of transactions to return
            transaction_type: 'expenses', 'income', or 'all'
            account_id: Optional account ID filter
        
        Returns:
            DataFrame with transaction details
        """
        start_date, end_date = self.parse_time_frame(time_frame)
        session = self.db_manager.get_session()
        
        try:
            # Build query
            query = session.query(Transaction).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date
                )
            )
            
            # Filter by transaction type
            if transaction_type == 'expenses':
                query = query.filter(Transaction.amount < 0).order_by(Transaction.amount.asc())
            elif transaction_type == 'income':
                query = query.filter(Transaction.amount > 0).order_by(Transaction.amount.desc())
            else:  # all
                query = query.order_by(func.abs(Transaction.amount).desc())
            
            # Apply account filter
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            
            # Limit results
            transactions = query.limit(limit).all()
            
            if not transactions:
                return pd.DataFrame(columns=['date', 'description', 'amount', 'category'])
            
            # Convert to DataFrame
            data = [{
                'date': t.date.strftime('%Y-%m-%d'),
                'description': t.description,
                'amount': t.amount,
                'category': t.category or 'Uncategorized'
            } for t in transactions]
            
            df = pd.DataFrame(data)
            logger.info(f"Retrieved top {len(df)} {transaction_type}")
            return df
        
        except Exception as e:
            logger.error(f"Failed to get top transactions: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def get_comparison_periods(
        self,
        periods: List[str]
    ) -> pd.DataFrame:
        """
        Compare spending across multiple time periods.
        
        Args:
            periods: List of time frame strings (e.g., ['1m', '3m', '6m'])
        
        Returns:
            DataFrame with period comparisons
        """
        comparisons = []
        
        for period in periods:
            try:
                summary = self.get_income_expense_summary(time_frame=period)
                comparisons.append({
                    'period': period,
                    'income': summary['total_income'],
                    'expenses': summary['total_expenses'],
                    'net': summary['net_change'],
                    'transactions': summary['total_count']
                })
            except Exception as e:
                logger.warning(f"Failed to get data for period {period}: {e}")
                comparisons.append({
                    'period': period,
                    'income': 0,
                    'expenses': 0,
                    'net': 0,
                    'transactions': 0
                })
        
        df = pd.DataFrame(comparisons)
        logger.info(f"Generated comparison for {len(periods)} periods")
        return df
    
    def get_transfers(
        self,
        time_frame: str = 'all',
        account_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get all internal transfers for analysis or review.
        
        Args:
            time_frame: Time frame for analysis
            account_id: Optional account ID filter
        
        Returns:
            DataFrame with transfer details: date, description, amount, account, category
        """
        start_date, end_date = self.parse_time_frame(time_frame)
        session = self.db_manager.get_session()
        
        try:
            # Build query for transfers only
            query = session.query(Transaction).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date,
                    Transaction.is_transfer == 1
                )
            )
            
            # Apply account filter if provided
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            
            # Order by date descending (most recent first)
            query = query.order_by(Transaction.date.desc())
            
            # Execute query
            transfers = query.all()
            
            if not transfers:
                return pd.DataFrame(columns=[
                    'id', 'date', 'description', 'amount', 'account', 'category'
                ])
            
            # Convert to DataFrame
            data = []
            for trans in transfers:
                data.append({
                    'id': trans.id,
                    'date': trans.date,
                    'description': trans.description,
                    'amount': trans.amount,
                    'account': trans.account or 'Unknown',
                    'category': trans.category or 'Transfer'
                })
            
            df = pd.DataFrame(data)
            logger.info(f"Retrieved {len(df)} transfers")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get transfers: {e}", exc_info=True)
            return pd.DataFrame(columns=[
                'id', 'date', 'description', 'amount', 'account', 'category'
            ])
        finally:
            session.close()

