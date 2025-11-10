"""
Comparison and reporting utilities for account verification.

This module provides functions to compare computed metrics against dashboard values
and generate formatted reports highlighting discrepancies.
"""

import logging
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import pandas as pd
from tabulate import tabulate


# Set up logging
logger = logging.getLogger(__name__)


@dataclass
class DashboardValues:
    """
    Container for expected dashboard values for an account.
    
    Attributes:
        account_name: Name of the account.
        income: Expected income value from dashboard.
        expenses: Expected expenses value from dashboard.
        net: Expected net value from dashboard.
        transaction_count: Expected transaction count from dashboard.
    """
    account_name: str
    income: float
    expenses: float
    net: float
    transaction_count: int


@dataclass
class Discrepancy:
    """
    Container for a detected discrepancy between computed and expected values.
    
    Attributes:
        account_name: Name of the account.
        metric: Name of the metric (e.g., 'income', 'expenses').
        computed_value: Value computed from database.
        expected_value: Value from dashboard.
        difference: Absolute difference between computed and expected.
        percent_difference: Percentage difference.
    """
    account_name: str
    metric: str
    computed_value: float
    expected_value: float
    difference: float
    percent_difference: float
    
    def __str__(self) -> str:
        """Format discrepancy as string."""
        return (
            f"{self.account_name} - {self.metric}: "
            f"Computed={self.computed_value:.2f}, "
            f"Expected={self.expected_value:.2f}, "
            f"Diff={self.difference:.2f} ({self.percent_difference:.1f}%)"
        )


def compare_metrics(
    computed_value: float,
    expected_value: float,
    tolerance: float = 0.01
) -> Tuple[bool, float, float]:
    """
    Compare two metric values with tolerance for floating-point errors.
    
    Args:
        computed_value: Value computed from database.
        expected_value: Expected value from dashboard.
        tolerance: Acceptable difference threshold (default 0.01).
        
    Returns:
        Tuple of (matches: bool, difference: float, percent_diff: float)
        - matches: True if values are within tolerance
        - difference: Absolute difference between values
        - percent_diff: Percentage difference (0-100 scale)
        
    Example:
        >>> matches, diff, pct = compare_metrics(1000.0, 1000.01, 0.02)
        >>> print(f"Matches: {matches}, Diff: {diff:.2f}")
        Matches: True, Diff: 0.01
    """
    difference = abs(computed_value - expected_value)
    
    # Calculate percentage difference
    # Avoid division by zero
    if expected_value != 0:
        percent_diff = (difference / abs(expected_value)) * 100
    else:
        # If expected is 0, use computed as denominator if non-zero
        if computed_value != 0:
            percent_diff = 100.0  # 100% difference if one is zero and other isn't
        else:
            percent_diff = 0.0  # Both are zero
    
    matches = difference <= tolerance
    
    return matches, difference, percent_diff


def compare_accounts(
    computed_metrics: Dict[str, Any],
    dashboard_values: Dict[str, DashboardValues],
    tolerance: float = 0.01
) -> Tuple[List[Discrepancy], bool]:
    """
    Compare computed metrics against dashboard values for all accounts.
    
    This function performs a comprehensive comparison of computed metrics
    against expected dashboard values, identifying discrepancies that exceed
    the specified tolerance threshold.
    
    Args:
        computed_metrics: Dictionary mapping account names to AccountMetrics objects.
        dashboard_values: Dictionary mapping account names to DashboardValues objects.
        tolerance: Acceptable difference threshold (default 0.01).
        
    Returns:
        Tuple of (discrepancies: List[Discrepancy], all_match: bool)
        - discrepancies: List of detected discrepancies
        - all_match: True if all accounts match within tolerance
        
    Example:
        >>> computed = compute_account_metrics(transactions_df)
        >>> dashboard = {'Checking': DashboardValues('Checking', 5000, 3000, 2000, 50)}
        >>> discrepancies, all_match = compare_accounts(computed, dashboard)
        >>> if not all_match:
        ...     for d in discrepancies:
        ...         print(d)
    """
    discrepancies = []
    
    # Get all unique account names from both sources
    all_accounts = set(computed_metrics.keys()) | set(dashboard_values.keys())
    
    logger.info(f"Comparing {len(all_accounts)} accounts")
    
    for account_name in sorted(all_accounts):
        # Check if account exists in both sources
        if account_name not in computed_metrics:
            logger.warning(f"Account '{account_name}' found in dashboard but not in computed metrics")
            discrepancies.append(Discrepancy(
                account_name=account_name,
                metric="missing_account",
                computed_value=0.0,
                expected_value=0.0,
                difference=0.0,
                percent_difference=0.0
            ))
            continue
        
        if account_name not in dashboard_values:
            logger.warning(f"Account '{account_name}' found in computed metrics but not in dashboard")
            discrepancies.append(Discrepancy(
                account_name=account_name,
                metric="unexpected_account",
                computed_value=0.0,
                expected_value=0.0,
                difference=0.0,
                percent_difference=0.0
            ))
            continue
        
        computed = computed_metrics[account_name]
        expected = dashboard_values[account_name]
        
        # Compare each metric
        metrics_to_compare = [
            ('income', computed.income, expected.income),
            ('expenses', computed.expenses, expected.expenses),
            ('net', computed.net, expected.net),
            ('transaction_count', float(computed.transaction_count), float(expected.transaction_count))
        ]
        
        for metric_name, computed_val, expected_val in metrics_to_compare:
            matches, diff, pct_diff = compare_metrics(computed_val, expected_val, tolerance)
            
            if not matches:
                discrepancy = Discrepancy(
                    account_name=account_name,
                    metric=metric_name,
                    computed_value=computed_val,
                    expected_value=expected_val,
                    difference=diff,
                    percent_difference=pct_diff
                )
                discrepancies.append(discrepancy)
                logger.warning(f"Discrepancy detected: {discrepancy}")
    
    all_match = len(discrepancies) == 0
    
    if all_match:
        logger.info("✓ All accounts match within tolerance")
    else:
        logger.warning(f"✗ Found {len(discrepancies)} discrepancies")
    
    return discrepancies, all_match


def generate_comparison_table(
    computed_metrics: Dict[str, Any],
    dashboard_values: Dict[str, DashboardValues],
    tolerance: float = 0.01
) -> pd.DataFrame:
    """
    Generate a formatted comparison table with computed vs expected values.
    
    Args:
        computed_metrics: Dictionary mapping account names to AccountMetrics objects.
        dashboard_values: Dictionary mapping account names to DashboardValues objects.
        tolerance: Acceptable difference threshold for highlighting.
        
    Returns:
        DataFrame with comparison data for display.
        
    Example:
        >>> df = generate_comparison_table(computed, dashboard, tolerance=0.01)
        >>> print(df.to_string())
    """
    rows = []
    
    # Get all unique account names
    all_accounts = sorted(set(computed_metrics.keys()) | set(dashboard_values.keys()))
    
    for account_name in all_accounts:
        computed = computed_metrics.get(account_name)
        expected = dashboard_values.get(account_name)
        
        if computed and expected:
            # Compare each metric
            income_match, income_diff, _ = compare_metrics(computed.income, expected.income, tolerance)
            expenses_match, expenses_diff, _ = compare_metrics(computed.expenses, expected.expenses, tolerance)
            net_match, net_diff, _ = compare_metrics(computed.net, expected.net, tolerance)
            count_match, count_diff, _ = compare_metrics(
                float(computed.transaction_count), 
                float(expected.transaction_count), 
                tolerance
            )
            
            row = {
                'Account': account_name,
                'Income (Computed)': f"${computed.income:,.2f}",
                'Income (Expected)': f"${expected.income:,.2f}",
                'Income Match': '✓' if income_match else f'✗ ({income_diff:.2f})',
                'Expenses (Computed)': f"${computed.expenses:,.2f}",
                'Expenses (Expected)': f"${expected.expenses:,.2f}",
                'Expenses Match': '✓' if expenses_match else f'✗ ({expenses_diff:.2f})',
                'Net (Computed)': f"${computed.net:,.2f}",
                'Net (Expected)': f"${expected.net:,.2f}",
                'Net Match': '✓' if net_match else f'✗ ({net_diff:.2f})',
                'Count (Computed)': computed.transaction_count,
                'Count (Expected)': expected.transaction_count,
                'Count Match': '✓' if count_match else f'✗ ({int(count_diff)})',
            }
        elif computed:
            row = {
                'Account': account_name,
                'Income (Computed)': f"${computed.income:,.2f}",
                'Income (Expected)': 'N/A',
                'Income Match': '?',
                'Expenses (Computed)': f"${computed.expenses:,.2f}",
                'Expenses (Expected)': 'N/A',
                'Expenses Match': '?',
                'Net (Computed)': f"${computed.net:,.2f}",
                'Net (Expected)': 'N/A',
                'Net Match': '?',
                'Count (Computed)': computed.transaction_count,
                'Count (Expected)': 'N/A',
                'Count Match': '?',
            }
        else:  # expected only
            row = {
                'Account': account_name,
                'Income (Computed)': 'N/A',
                'Income (Expected)': f"${expected.income:,.2f}",
                'Income Match': '?',
                'Expenses (Computed)': 'N/A',
                'Expenses (Expected)': f"${expected.expenses:,.2f}",
                'Expenses Match': '?',
                'Net (Computed)': 'N/A',
                'Net (Expected)': f"${expected.net:,.2f}",
                'Net Match': '?',
                'Count (Computed)': 'N/A',
                'Count (Expected)': expected.transaction_count,
                'Count Match': '?',
            }
        
        rows.append(row)
    
    return pd.DataFrame(rows)


def print_comparison_report(
    computed_metrics: Dict[str, Any],
    dashboard_values: Dict[str, DashboardValues],
    tolerance: float = 0.01,
    show_full_table: bool = True
) -> None:
    """
    Print a formatted comparison report to console.
    
    This function generates and displays a comprehensive comparison report,
    including a full comparison table and a summary of discrepancies.
    
    Args:
        computed_metrics: Dictionary mapping account names to AccountMetrics objects.
        dashboard_values: Dictionary mapping account names to DashboardValues objects.
        tolerance: Acceptable difference threshold.
        show_full_table: If True, display full comparison table; otherwise show summary only.
        
    Example:
        >>> print_comparison_report(computed, dashboard, tolerance=0.01)
    """
    print("\n" + "=" * 100)
    print("ACCOUNT VERIFICATION REPORT")
    print("=" * 100 + "\n")
    
    # Get discrepancies
    discrepancies, all_match = compare_accounts(computed_metrics, dashboard_values, tolerance)
    
    if show_full_table:
        # Generate and print full comparison table
        df = generate_comparison_table(computed_metrics, dashboard_values, tolerance)
        print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
        print("\n")
    
    # Print summary
    print("-" * 100)
    print("SUMMARY")
    print("-" * 100)
    print(f"Total accounts compared: {len(set(computed_metrics.keys()) | set(dashboard_values.keys()))}")
    print(f"Tolerance threshold: ±${tolerance:.2f}")
    print(f"Discrepancies found: {len(discrepancies)}")
    
    if all_match:
        print("\n✓ SUCCESS: All accounts match within tolerance!")
    else:
        print(f"\n✗ FAILURE: {len(discrepancies)} discrepancies detected")
        print("\nDiscrepancy Details:")
        print("-" * 100)
        
        for i, disc in enumerate(discrepancies, 1):
            if disc.metric in ['missing_account', 'unexpected_account']:
                print(f"{i}. {disc.account_name}: {disc.metric.replace('_', ' ').title()}")
            else:
                print(f"{i}. {disc}")
    
    print("\n" + "=" * 100 + "\n")


def export_report_to_csv(
    computed_metrics: Dict[str, Any],
    dashboard_values: Dict[str, DashboardValues],
    output_path: str,
    tolerance: float = 0.01
) -> None:
    """
    Export comparison report to CSV file.
    
    Args:
        computed_metrics: Dictionary mapping account names to AccountMetrics objects.
        dashboard_values: Dictionary mapping account names to DashboardValues objects.
        output_path: Path where CSV file should be saved.
        tolerance: Acceptable difference threshold.
        
    Example:
        >>> export_report_to_csv(computed, dashboard, 'report.csv')
    """
    df = generate_comparison_table(computed_metrics, dashboard_values, tolerance)
    df.to_csv(output_path, index=False)
    logger.info(f"Report exported to {output_path}")
    print(f"\n✓ Report exported to: {output_path}")

