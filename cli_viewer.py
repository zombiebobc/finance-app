"""
Command-line interface viewer for financial transactions.

This module provides a CLI for viewing transactions using argparse
and displays data in a readable tabular format using tabulate.
"""

import argparse
import logging
import sys
from typing import Optional, Dict, Any
import pandas as pd
from tabulate import tabulate

from data_viewer import DataViewer
from database_ops import DatabaseManager

# Configure logging
logger = logging.getLogger(__name__)


def parse_cli_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the viewer.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="View financial transactions from the database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # View all transactions
  python main.py view
  
  # Filter by date range
  python main.py view --date-start 2024-01-01 --date-end 2024-12-31
  
  # Filter by amount range
  python main.py view --amount-min -100 --amount-max 1000
  
  # Search by description
  python main.py view --description "AMAZON"
  
  # Filter by category
  python main.py view --category "Shopping"
  
  # Limit results and export
  python main.py view --limit 50 --export transactions.csv
        """
    )
    
    # Date filters
    parser.add_argument(
        "--date-start",
        type=str,
        help="Start date (YYYY-MM-DD format, inclusive)"
    )
    parser.add_argument(
        "--date-end",
        type=str,
        help="End date (YYYY-MM-DD format, inclusive)"
    )
    
    # Amount filters
    parser.add_argument(
        "--amount-min",
        type=float,
        help="Minimum amount (inclusive)"
    )
    parser.add_argument(
        "--amount-max",
        type=float,
        help="Maximum amount (inclusive)"
    )
    
    # Description filter
    parser.add_argument(
        "--description",
        type=str,
        help="Search keywords in description (case-insensitive)"
    )
    
    # Category filter
    parser.add_argument(
        "--category",
        type=str,
        help="Filter by category (case-insensitive partial match)"
    )
    
    # Source file filter
    parser.add_argument(
        "--source-file",
        type=str,
        help="Filter by source file name (case-insensitive partial match)"
    )
    
    # Account filter
    parser.add_argument(
        "--account-id",
        type=int,
        help="Filter by account ID"
    )
    parser.add_argument(
        "--account-name",
        type=str,
        help="Filter by account name (case-insensitive partial match)"
    )
    
    # Display options
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of transactions to display"
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Number of transactions to skip (for pagination)"
    )
    parser.add_argument(
        "--sort-by",
        type=str,
        default="date",
        choices=["id", "date", "description", "amount", "category", "source_file"],
        help="Column to sort by (default: date)"
    )
    parser.add_argument(
        "--sort-asc",
        action="store_true",
        help="Sort ascending (default: descending)"
    )
    
    # Export option
    parser.add_argument(
        "--export",
        type=str,
        metavar="FILE",
        help="Export results to CSV file"
    )
    
    # Summary statistics
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show summary statistics"
    )
    
    return parser.parse_args()


def build_filters(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Build filter dictionary from parsed arguments.
    
    Args:
        args: Parsed command-line arguments
    
    Returns:
        Dictionary of filter criteria
    """
    filters = {}
    
    if args.date_start:
        filters["date_start"] = args.date_start
    if args.date_end:
        filters["date_end"] = args.date_end
    if args.amount_min is not None:
        filters["amount_min"] = args.amount_min
    if args.amount_max is not None:
        filters["amount_max"] = args.amount_max
    if args.description:
        filters["description_keywords"] = args.description
    if args.category:
        filters["category"] = args.category
    if args.source_file:
        filters["source_file"] = args.source_file
    if args.account_id is not None:
        filters["account_id"] = args.account_id
    if args.account_name:
        filters["account_name"] = args.account_name
    
    return filters


def format_amount(amount: float) -> str:
    """
    Format amount as currency string.
    
    Args:
        amount: Transaction amount (can be None)
    
    Returns:
        Formatted string (e.g., "$1,234.56" or "-$123.45")
    """
    if amount is None or pd.isna(amount):
        return "$0.00"
    return f"${float(amount):,.2f}"


def display_transactions(
    viewer: DataViewer,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    offset: int = 0,
    sort_by: str = "date",
    sort_asc: bool = False
) -> None:
    """
    Display transactions in a formatted table.
    
    Args:
        viewer: DataViewer instance
        filters: Optional filter criteria
        limit: Maximum number of records to display
        offset: Number of records to skip
        sort_by: Column to sort by
        sort_asc: If True, sort ascending; if False, descending
    """
    try:
        # Get transactions as DataFrame
        df = viewer.get_transactions_df(
            filters=filters,
            limit=limit,
            offset=offset,
            order_by=sort_by,
            order_desc=not sort_asc
        )
        
        if df.empty:
            print("\nNo transactions found matching the criteria.")
            return
        
        # Format for display
        formatted_df = viewer.format_transactions_df(df)
        
        # Prepare display columns
        display_df = formatted_df[[
            "date", "description", "amount", "category", "source_file"
        ]].copy()
        
        # Format amount column
        display_df["amount"] = display_df["amount"].apply(format_amount)
        
        # Truncate long descriptions
        display_df["description"] = display_df["description"].apply(
            lambda x: x[:50] + "..." if len(x) > 50 else x
        )
        
        # Rename columns for display
        display_df.columns = ["Date", "Description", "Amount", "Category", "Source File"]
        
        # Display table
        print("\n" + "=" * 120)
        print(f"TRANSACTIONS ({len(df)} shown)")
        print("=" * 120)
        try:
            # Convert DataFrame to list of lists for tabulate
            table_data = display_df.values.tolist()
            headers = display_df.columns.tolist()
            print(tabulate(
                table_data,
                headers=headers,
                tablefmt="grid",
                showindex=False
            ))
        except Exception as e:
            # Fallback: print DataFrame directly
            logger.warning(f"Tabulate error, using pandas display: {e}")
            print(display_df.to_string(index=False))
        print("=" * 120)
        
        # Show pagination info if applicable
        if limit:
            print(f"\nShowing {len(df)} transactions (limit: {limit}, offset: {offset})")
        
    except Exception as e:
        logger.error(f"Failed to display transactions: {e}")
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


def display_stats(viewer: DataViewer, filters: Optional[Dict[str, Any]] = None) -> None:
    """
    Display summary statistics.
    
    Args:
        viewer: DataViewer instance
        filters: Optional filter criteria
    """
    try:
        stats = viewer.get_summary_stats(filters=filters)
        
        print("\n" + "=" * 60)
        print("SUMMARY STATISTICS")
        print("=" * 60)
        print(f"Total Transactions:     {stats['total_count']:,}")
        print(f"Total Amount:          {format_amount(stats['total_amount'])}")
        print(f"Average Amount:       {format_amount(stats['average_amount'])}")
        print(f"Minimum Amount:       {format_amount(stats['min_amount'])}")
        print(f"Maximum Amount:       {format_amount(stats['max_amount'])}")
        print()
        print(f"Credits (Positive):   {stats['positive_count']:,} transactions")
        print(f"  Total:              {format_amount(stats['positive_total'])}")
        print(f"Debits (Negative):    {stats['negative_count']:,} transactions")
        print(f"  Total:              {format_amount(stats['negative_total'])}")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Failed to display statistics: {e}")
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


def main_cli_viewer(connection_string: str, args: Optional[argparse.Namespace] = None) -> None:
    """
    Main entry point for CLI viewer.
    
    Args:
        connection_string: Database connection string
        args: Optional parsed arguments (if None, will parse from command line)
    """
    if args is None:
        args = parse_cli_args()
    
    # Initialize database and viewer
    try:
        db_manager = DatabaseManager(connection_string)
        viewer = DataViewer(db_manager)
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Build filters
        filters = build_filters(args) if args else {}
        
        # Display statistics if requested
        if args and args.stats:
            display_stats(viewer, filters)
            print()  # Add spacing
        
        # Display transactions
        display_transactions(
            viewer,
            filters=filters if args else None,
            limit=args.limit if args else None,
            offset=args.offset if args else 0,
            sort_by=args.sort_by if args else "date",
            sort_asc=args.sort_asc if args else False
        )
        
        # Export if requested
        if args and args.export:
            try:
                df = viewer.get_transactions_df(
                    filters=filters,
                    limit=args.limit,
                    offset=args.offset,
                    order_by=args.sort_by,
                    order_desc=not args.sort_asc
                )
                viewer.export_to_csv(df, args.export)
                print(f"\nExported {len(df)} transactions to {args.export}")
            except Exception as e:
                print(f"\nError exporting to CSV: {e}", file=sys.stderr)
                sys.exit(1)
        
    except ValueError as e:
        print(f"\nInvalid filter: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error in CLI viewer: {e}", exc_info=True)
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db_manager.close()

