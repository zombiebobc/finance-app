"""
Financial data analyzer for computing account summaries.

This module provides functions to aggregate transaction data and compute
key financial metrics including income, expenses, net amounts, and transaction counts.
"""

import logging
from typing import Dict, Any
import pandas as pd
import numpy as np


# Set up logging
logger = logging.getLogger(__name__)


class AccountMetrics:
    """
    Container class for account financial metrics.
    
    Attributes:
        account_name: Name of the account.
        income: Total income (sum of positive amounts).
        expenses: Total expenses (absolute value of negative amounts).
        net: Net amount (income - expenses).
        transaction_count: Number of transactions.
    """
    
    def __init__(
        self,
        account_name: str,
        income: float,
        expenses: float,
        net: float,
        transaction_count: int
    ):
        """
        Initialize AccountMetrics.
        
        Args:
            account_name: Name of the account.
            income: Total income amount.
            expenses: Total expenses amount (should be positive).
            net: Net amount (income - expenses).
            transaction_count: Number of transactions.
        """
        self.account_name = account_name
        self.income = income
        self.expenses = expenses
        self.net = net
        self.transaction_count = transaction_count
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert metrics to dictionary format.
        
        Returns:
            Dict containing all metrics.
        """
        return {
            'account_name': self.account_name,
            'income': self.income,
            'expenses': self.expenses,
            'net': self.net,
            'transaction_count': self.transaction_count
        }
    
    def __repr__(self) -> str:
        """String representation of metrics."""
        return (
            f"AccountMetrics(account='{self.account_name}', "
            f"income={self.income:.2f}, expenses={self.expenses:.2f}, "
            f"net={self.net:.2f}, count={self.transaction_count})"
        )


def compute_account_metrics(transactions_df: pd.DataFrame) -> Dict[str, AccountMetrics]:
    """
    Compute financial metrics for each account from transaction data.
    
    This function aggregates transaction data by account, computing:
    - Total income (sum of positive amounts)
    - Total expenses (absolute value of sum of negative amounts)
    - Net amount (income - expenses)
    - Transaction count
    
    Args:
        transactions_df: DataFrame with columns: account_name, date, amount, 
                        category, type. Amount should be positive for income,
                        negative for expenses.
                        
    Returns:
        Dict mapping account names to AccountMetrics objects.
        
    Raises:
        ValueError: If DataFrame is empty or missing required columns.
        
    Example:
        >>> df = pd.DataFrame({
        ...     'account_name': ['Checking', 'Checking', 'Savings'],
        ...     'amount': [1000.0, -500.0, 2000.0],
        ...     'date': pd.date_range('2024-01-01', periods=3),
        ...     'category': ['Salary', 'Groceries', 'Interest'],
        ...     'type': ['income', 'expense', 'income']
        ... })
        >>> metrics = compute_account_metrics(df)
        >>> print(metrics['Checking'].income)
        1000.0
    """
    # Validate input
    if transactions_df.empty:
        logger.warning("Empty DataFrame provided, returning empty metrics")
        return {}
    
    required_columns = {'account_name', 'amount'}
    missing_columns = required_columns - set(transactions_df.columns)
    if missing_columns:
        raise ValueError(f"DataFrame missing required columns: {missing_columns}")
    
    logger.info(f"Computing metrics for {len(transactions_df)} transactions")
    
    # Group by account name
    account_groups = transactions_df.groupby('account_name')
    
    metrics_dict = {}
    
    for account_name, group in account_groups:
        # Separate income and expenses based on amount sign
        # Income: positive amounts
        income_mask = group['amount'] > 0
        income = group.loc[income_mask, 'amount'].sum()
        
        # Expenses: negative amounts (convert to positive for display)
        expense_mask = group['amount'] < 0
        expenses = abs(group.loc[expense_mask, 'amount'].sum())
        
        # Net: income minus expenses (or simply sum of all amounts)
        net = group['amount'].sum()
        
        # Transaction count
        transaction_count = len(group)
        
        # Create metrics object
        metrics = AccountMetrics(
            account_name=str(account_name),
            income=float(income),
            expenses=float(expenses),
            net=float(net),
            transaction_count=int(transaction_count)
        )
        
        metrics_dict[str(account_name)] = metrics
        
        logger.debug(
            f"Account: {account_name} - "
            f"Income: ${income:.2f}, Expenses: ${expenses:.2f}, "
            f"Net: ${net:.2f}, Count: {transaction_count}"
        )
    
    logger.info(f"Computed metrics for {len(metrics_dict)} accounts")
    return metrics_dict


def aggregate_by_category(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate transactions by category.
    
    This function groups transactions by category and computes total amounts
    for each category. Useful for future expansions to verify category-level
    dashboard sections.
    
    Args:
        transactions_df: DataFrame with transaction data including 'category' 
                        and 'amount' columns.
                        
    Returns:
        DataFrame with columns: category, total_amount, transaction_count.
        
    Example:
        >>> df = pd.DataFrame({
        ...     'category': ['Groceries', 'Salary', 'Groceries'],
        ...     'amount': [-100.0, 2000.0, -150.0]
        ... })
        >>> agg = aggregate_by_category(df)
        >>> print(agg)
    """
    if transactions_df.empty:
        logger.warning("Empty DataFrame provided for category aggregation")
        return pd.DataFrame(columns=['category', 'total_amount', 'transaction_count'])
    
    if 'category' not in transactions_df.columns:
        raise ValueError("DataFrame missing 'category' column")
    
    # Group by category and aggregate
    category_agg = transactions_df.groupby('category').agg(
        total_amount=('amount', 'sum'),
        transaction_count=('amount', 'count')
    ).reset_index()
    
    # Sort by absolute amount (largest first)
    category_agg['abs_amount'] = category_agg['total_amount'].abs()
    category_agg = category_agg.sort_values('abs_amount', ascending=False)
    category_agg = category_agg.drop('abs_amount', axis=1)
    
    logger.info(f"Aggregated {len(category_agg)} categories")
    return category_agg


def compute_summary_statistics(metrics_dict: Dict[str, AccountMetrics]) -> Dict[str, Any]:
    """
    Compute summary statistics across all accounts.
    
    Args:
        metrics_dict: Dictionary mapping account names to AccountMetrics objects.
        
    Returns:
        Dict containing summary statistics:
            - total_accounts: Number of accounts
            - total_income: Sum of income across all accounts
            - total_expenses: Sum of expenses across all accounts
            - total_net: Sum of net amounts across all accounts
            - total_transactions: Total number of transactions
            
    Example:
        >>> metrics = compute_account_metrics(df)
        >>> summary = compute_summary_statistics(metrics)
        >>> print(f"Total income: ${summary['total_income']:.2f}")
    """
    if not metrics_dict:
        logger.warning("Empty metrics dictionary provided")
        return {
            'total_accounts': 0,
            'total_income': 0.0,
            'total_expenses': 0.0,
            'total_net': 0.0,
            'total_transactions': 0
        }
    
    summary = {
        'total_accounts': len(metrics_dict),
        'total_income': sum(m.income for m in metrics_dict.values()),
        'total_expenses': sum(m.expenses for m in metrics_dict.values()),
        'total_net': sum(m.net for m in metrics_dict.values()),
        'total_transactions': sum(m.transaction_count for m in metrics_dict.values())
    }
    
    logger.info(f"Summary: {summary['total_accounts']} accounts, "
                f"{summary['total_transactions']} transactions, "
                f"${summary['total_net']:.2f} net")
    
    return summary


def validate_amount_consistency(transactions_df: pd.DataFrame) -> bool:
    """
    Validate that transaction amounts follow expected conventions.
    
    Checks for potential data quality issues such as:
    - Presence of NaN or infinite values
    - Unexpectedly large amounts (potential data errors)
    
    Args:
        transactions_df: DataFrame with transaction data.
        
    Returns:
        bool: True if data passes validation checks, False otherwise.
    """
    if transactions_df.empty:
        return True
    
    issues = []
    
    # Check for NaN or infinite values
    if transactions_df['amount'].isna().any():
        nan_count = transactions_df['amount'].isna().sum()
        issues.append(f"Found {nan_count} NaN values in amounts")
    
    if np.isinf(transactions_df['amount']).any():
        inf_count = np.isinf(transactions_df['amount']).sum()
        issues.append(f"Found {inf_count} infinite values in amounts")
    
    # Check for suspiciously large amounts (> $1 million)
    large_threshold = 1_000_000
    large_amounts = transactions_df[transactions_df['amount'].abs() > large_threshold]
    if not large_amounts.empty:
        issues.append(
            f"Found {len(large_amounts)} transactions with amounts > ${large_threshold:,}"
        )
        logger.warning(f"Large amounts detected:\n{large_amounts[['account_name', 'amount', 'date']].head()}")
    
    if issues:
        logger.warning("Data validation issues detected:")
        for issue in issues:
            logger.warning(f"  - {issue}")
        return False
    
    logger.info("Amount validation passed")
    return True

