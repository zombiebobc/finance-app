"""
Main script for verifying account summaries against database.

This script orchestrates the account verification process by:
1. Connecting to the database
2. Querying transactions within the specified date range
3. Computing metrics from the data
4. Comparing against hardcoded dashboard values
5. Generating a comparison report

Usage:
    python verify_accounts.py --db_path finances.db
    python verify_accounts.py --db_path finances.db --include_transfers
    python verify_accounts.py --db_path finances.db --start_date 2024-01-01 --end_date 2024-12-31
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from typing import Dict

from db_utils import (
    connect_to_database,
    query_transactions,
    verify_database_schema,
    close_connection
)
from analyzer import (
    compute_account_metrics,
    compute_summary_statistics,
    validate_amount_consistency
)
from reporter import (
    DashboardValues,
    compare_accounts,
    print_comparison_report,
    export_report_to_csv
)


# Configure logging
def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


# Hardcoded dashboard values for comparison
# In a real implementation, these would be fetched from the dashboard API or config
def get_dashboard_values() -> Dict[str, DashboardValues]:
    """
    Get expected dashboard values for comparison.
    
    These values represent what the dashboard currently displays.
    In production, these would be fetched from the dashboard's API or database.
    
    Returns:
        Dictionary mapping account names to DashboardValues objects.
    """
    # Example hardcoded values - replace with actual dashboard values
    dashboard_data = {
        'Checking Account': DashboardValues(
            account_name='Checking Account',
            income=50000.00,
            expenses=35000.00,
            net=15000.00,
            transaction_count=250
        ),
        'Savings Account': DashboardValues(
            account_name='Savings Account',
            income=12000.00,
            expenses=2000.00,
            net=10000.00,
            transaction_count=45
        ),
        'Credit Card': DashboardValues(
            account_name='Credit Card',
            income=500.00,  # Cashback/rewards
            expenses=18000.00,
            net=-17500.00,
            transaction_count=180
        ),
    }
    
    return dashboard_data


def parse_date(date_string: str) -> date:
    """
    Parse date string in YYYY-MM-DD format.
    
    Args:
        date_string: Date in YYYY-MM-DD format.
        
    Returns:
        date object.
        
    Raises:
        ValueError: If date format is invalid.
    """
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(f"Invalid date format '{date_string}'. Use YYYY-MM-DD format.") from e


def calculate_rolling_12_months(end_date: date) -> tuple[date, date]:
    """
    Calculate start and end dates for rolling 12-month period.
    
    Args:
        end_date: End date (inclusive).
        
    Returns:
        Tuple of (start_date, end_date) for 12-month period.
    """
    # Start date is 12 months before end date (plus one day to be exclusive of start)
    start_date = end_date - timedelta(days=365) + timedelta(days=1)
    return start_date, end_date


def main() -> int:
    """
    Main function to orchestrate account verification.
    
    Returns:
        Exit code: 0 for success, 1 for failure.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Verify account summaries by comparing database values to dashboard values.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with default settings (last 12 months, exclude transfers)
  python verify_accounts.py --db_path finances.db
  
  # Include transfers in calculations
  python verify_accounts.py --db_path finances.db --include_transfers
  
  # Custom date range
  python verify_accounts.py --db_path finances.db --start_date 2024-01-01 --end_date 2024-12-31
  
  # Filter to specific account
  python verify_accounts.py --db_path finances.db --account "Checking Account"
  
  # Export results to CSV
  python verify_accounts.py --db_path finances.db --export report.csv
  
  # Debug mode with verbose logging
  python verify_accounts.py --db_path finances.db --log_level DEBUG
        """
    )
    
    parser.add_argument(
        '--db_path',
        type=str,
        required=True,
        help='Path to SQLite database file'
    )
    
    parser.add_argument(
        '--start_date',
        type=str,
        default=None,
        help='Start date for analysis (YYYY-MM-DD). Defaults to 12 months before end_date.'
    )
    
    parser.add_argument(
        '--end_date',
        type=str,
        default=None,
        help='End date for analysis (YYYY-MM-DD). Defaults to November 10, 2025.'
    )
    
    parser.add_argument(
        '--include_transfers',
        action='store_true',
        help='Include transfer transactions in calculations (default: exclude)'
    )
    
    parser.add_argument(
        '--account',
        type=str,
        default=None,
        help='Filter to specific account name'
    )
    
    parser.add_argument(
        '--tolerance',
        type=float,
        default=0.01,
        help='Tolerance for floating-point comparison (default: 0.01)'
    )
    
    parser.add_argument(
        '--export',
        type=str,
        default=None,
        help='Export comparison report to CSV file'
    )
    
    parser.add_argument(
        '--log_level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("Account Verification Script Started")
    logger.info("=" * 80)
    
    try:
        # Parse dates
        if args.end_date:
            end_date = parse_date(args.end_date)
        else:
            # Default to November 10, 2025
            end_date = date(2025, 11, 10)
        
        if args.start_date:
            start_date = parse_date(args.start_date)
        else:
            # Calculate rolling 12 months
            start_date, end_date = calculate_rolling_12_months(end_date)
        
        logger.info(f"Analysis period: {start_date} to {end_date}")
        logger.info(f"Include transfers: {args.include_transfers}")
        logger.info(f"Account filter: {args.account if args.account else 'None (all accounts)'}")
        logger.info(f"Tolerance: ${args.tolerance:.2f}")
        
        # Connect to database
        logger.info(f"Connecting to database: {args.db_path}")
        conn = connect_to_database(args.db_path)
        
        # Verify database schema
        logger.info("Verifying database schema...")
        if not verify_database_schema(conn):
            logger.error("Database schema verification failed")
            print("\n✗ ERROR: Database schema is invalid or missing required columns.")
            print("Expected columns: account_name, date, amount, category, type")
            return 1
        
        # Query transactions
        logger.info("Querying transactions...")
        transactions_df = query_transactions(
            conn=conn,
            start_date=start_date,
            end_date=end_date,
            exclude_transfers=not args.include_transfers,
            account_filter=args.account
        )
        
        # Check if we got any data
        if transactions_df.empty:
            logger.warning("No transactions found matching the criteria")
            print("\n⚠ WARNING: No transactions found matching the specified criteria.")
            print(f"  Date range: {start_date} to {end_date}")
            print(f"  Account filter: {args.account if args.account else 'None'}")
            print(f"  Include transfers: {args.include_transfers}")
            return 0
        
        logger.info(f"Retrieved {len(transactions_df)} transactions")
        
        # Validate data
        logger.info("Validating transaction data...")
        if not validate_amount_consistency(transactions_df):
            logger.warning("Data validation found potential issues, but continuing...")
        
        # Compute metrics
        logger.info("Computing account metrics...")
        computed_metrics = compute_account_metrics(transactions_df)
        
        if not computed_metrics:
            logger.error("Failed to compute metrics")
            print("\n✗ ERROR: Failed to compute metrics from transactions.")
            return 1
        
        # Get summary statistics
        summary = compute_summary_statistics(computed_metrics)
        logger.info(f"Computed metrics for {summary['total_accounts']} accounts")
        
        # Get dashboard values for comparison
        logger.info("Loading dashboard values...")
        dashboard_values = get_dashboard_values()
        
        # If account filter is specified, filter dashboard values too
        if args.account:
            dashboard_values = {
                k: v for k, v in dashboard_values.items() 
                if k == args.account
            }
        
        # Compare and generate report
        logger.info("Generating comparison report...")
        print_comparison_report(
            computed_metrics=computed_metrics,
            dashboard_values=dashboard_values,
            tolerance=args.tolerance,
            show_full_table=True
        )
        
        # Export to CSV if requested
        if args.export:
            logger.info(f"Exporting report to {args.export}")
            export_report_to_csv(
                computed_metrics=computed_metrics,
                dashboard_values=dashboard_values,
                output_path=args.export,
                tolerance=args.tolerance
            )
        
        # Check if verification passed
        discrepancies, all_match = compare_accounts(
            computed_metrics, 
            dashboard_values, 
            args.tolerance
        )
        
        # Close database connection
        close_connection(conn)
        
        logger.info("=" * 80)
        logger.info("Account Verification Script Completed")
        logger.info("=" * 80)
        
        # Return appropriate exit code
        return 0 if all_match else 1
        
    except FileNotFoundError as e:
        logger.error(f"Database file not found: {e}")
        print(f"\n✗ ERROR: Database file not found: {args.db_path}")
        return 1
    except ValueError as e:
        logger.error(f"Invalid input: {e}")
        print(f"\n✗ ERROR: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\n✗ ERROR: An unexpected error occurred: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())

