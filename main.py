"""
Main module for financial transaction CSV importer.

This module orchestrates the import process:
1. Reads CSV files
2. Standardizes data
3. Detects duplicates
4. Inserts into database
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List
import yaml

from data_ingestion import CSVReader
from data_standardization import DataStandardizer
from duplicate_detection import DuplicateDetector
from database_ops import DatabaseManager
from utils import ensure_data_dir, resolve_connection_string, resolve_log_path

# Configure module-level logger
logger = logging.getLogger(__name__)


def setup_logging(config: dict) -> None:
    """
    Configure logging based on config settings.
    
    Args:
        config: Configuration dictionary with logging settings
    """
    log_config = config.get("logging", {})
    log_level = getattr(logging, log_config.get("level", "INFO").upper())
    log_format = log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    log_file = log_config.get("file")
    
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        try:
            log_path = resolve_log_path(log_file)
        except OSError as exc:
            raise RuntimeError(f"Unable to prepare log file path '{log_file}': {exc}") from exc
        handlers.append(logging.FileHandler(log_path))
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers
    )


def load_config(config_path: Path) -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config.yaml file
    
    Returns:
        Configuration dictionary
    
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid YAML
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def create_connection_string(config: dict) -> str:
    """
    Create SQLAlchemy connection string from config.
    
    Args:
        config: Configuration dictionary with database settings
    
    Returns:
        SQLAlchemy connection string
    """
    return resolve_connection_string(config)


def import_transactions(
    file_paths: List[Path],
    config: dict,
    db_manager: DatabaseManager
) -> dict:
    """
    Import transactions from CSV files.
    
    Args:
        file_paths: List of paths to CSV files
        config: Configuration dictionary
        db_manager: DatabaseManager instance
    
    Returns:
        Dictionary with import statistics
    """
    # Initialize components
    processing_cfg = config.get("processing", {})

    csv_reader = CSVReader(
        chunk_size=processing_cfg.get("chunk_size", 10000),
        auto_chunk_mb=processing_cfg.get("auto_chunk_mb", 25),
        skip_on_error=processing_cfg.get("skip_on_error", True),
    )
    
    standardizer = DataStandardizer(
        column_mappings=config.get("column_mappings", {}),
        date_formats=processing_cfg.get("date_formats", []),
        output_date_format=processing_cfg.get("output_date_format", "%Y-%m-%d"),
        amount_decimal_places=processing_cfg.get("amount_decimal_places", 2),
        max_error_rows=processing_cfg.get("max_error_rows"),
        max_error_ratio=processing_cfg.get("error_ratio", 0.1),
        fallback_values=processing_cfg.get("fallback_values"),
    )
    
    duplicate_detector = DuplicateDetector(
        key_fields=config.get("duplicate_detection", {}).get("key_fields", ["date", "description", "amount"]),
        hash_algorithm=config.get("duplicate_detection", {}).get("hash_algorithm", "md5")
    )
    
    # Statistics
    stats = {
        "files_processed": 0,
        "files_failed": 0,
        "total_rows_read": 0,
        "total_transactions_standardized": 0,
        "total_duplicates_found": 0,
        "total_inserted": 0,
        "total_skipped": 0,
        "errors": []
    }
    
    # Process each file
    for file_path in file_paths:
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Validate file
            is_valid, error_msg = csv_reader.validate_csv(file_path)
            if not is_valid:
                logger.error(f"Invalid CSV file {file_path}: {error_msg}")
                stats["files_failed"] += 1
                stats["errors"].append(f"{file_path}: {error_msg}")
                continue
            
            # Read CSV (handle large files with chunking)
            file_stats = {
                "rows_read": 0,
                "transactions_standardized": 0,
                "duplicates_found": 0,
                "inserted": 0,
                "skipped": 0
            }
            
            # Determine if we should chunk (based on file size or explicit setting)
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            use_chunking = file_size_mb > 10  # Chunk if file > 10MB
            
            if use_chunking:
                logger.info(f"Processing large file in chunks: {file_path}")
                chunk_iterator = csv_reader.read_csv(file_path, chunked=True, on_error="prompt")
                
                for chunk_df in chunk_iterator:
                    file_stats["rows_read"] += len(chunk_df)
                    
                    # Standardize chunk
                    try:
                        standardized = standardizer.standardize_dataframe(
                            chunk_df,
                            source_file=str(file_path.name)
                        )
                        file_stats["transactions_standardized"] += len(standardized)
                    except ValueError as e:
                        logger.error(f"Standardization failed for chunk: {e}")
                        stats["errors"].append(f"{file_path} (chunk): {str(e)}")
                        continue
                    
                    if not standardized:
                        continue
                    
                    # Generate hashes
                    hashes = duplicate_detector.generate_hashes_batch(standardized)
                    standardized_with_hashes = [
                        {**trans, "duplicate_hash": hash_val}
                        for trans, hash_val in zip(standardized, hashes)
                        if hash_val is not None
                    ]
                    
                    # Check for duplicates
                    existing_hashes = set(
                        db_manager.check_duplicate_hashes([t["duplicate_hash"] for t in standardized_with_hashes])
                    )
                    
                    unique_transactions, duplicate_transactions = duplicate_detector.filter_duplicates(
                        standardized_with_hashes,
                        existing_hashes
                    )
                    
                    file_stats["duplicates_found"] += len(duplicate_transactions)
                    
                    # Insert unique transactions
                    if unique_transactions:
                        inserted, skipped = db_manager.insert_transactions(unique_transactions)
                        file_stats["inserted"] += inserted
                        file_stats["skipped"] += skipped
            else:
                # Process entire file at once
                df = csv_reader.read_csv(file_path, chunked=False, on_error="prompt")
                file_stats["rows_read"] = len(df)
                
                # Standardize
                try:
                    standardized = standardizer.standardize_dataframe(
                        df,
                        source_file=str(file_path.name)
                    )
                    file_stats["transactions_standardized"] = len(standardized)
                except ValueError as e:
                    logger.error(f"Standardization failed: {e}")
                    stats["files_failed"] += 1
                    stats["errors"].append(f"{file_path}: {str(e)}")
                    continue
                
                if not standardized:
                    logger.warning(f"No valid transactions found in {file_path}")
                    stats["files_processed"] += 1
                    continue
                
                # Generate hashes
                hashes = duplicate_detector.generate_hashes_batch(standardized)
                standardized_with_hashes = [
                    {**trans, "duplicate_hash": hash_val}
                    for trans, hash_val in zip(standardized, hashes)
                    if hash_val is not None
                ]
                
                # Check for duplicates
                existing_hashes = set(
                    db_manager.check_duplicate_hashes([t["duplicate_hash"] for t in standardized_with_hashes])
                )
                
                unique_transactions, duplicate_transactions = duplicate_detector.filter_duplicates(
                    standardized_with_hashes,
                    existing_hashes
                )
                
                file_stats["duplicates_found"] = len(duplicate_transactions)
                
                # Insert unique transactions
                if unique_transactions:
                    inserted, skipped = db_manager.insert_transactions(unique_transactions)
                    file_stats["inserted"] = inserted
                    file_stats["skipped"] = skipped
            
            # Update overall statistics
            stats["files_processed"] += 1
            stats["total_rows_read"] += file_stats["rows_read"]
            stats["total_transactions_standardized"] += file_stats["transactions_standardized"]
            stats["total_duplicates_found"] += file_stats["duplicates_found"]
            stats["total_inserted"] += file_stats["inserted"]
            stats["total_skipped"] += file_stats["skipped"]
            
            logger.info(
                f"File {file_path.name} processed: "
                f"{file_stats['inserted']} inserted, "
                f"{file_stats['duplicates_found']} duplicates, "
                f"{file_stats['skipped']} skipped"
            )
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
            stats["files_failed"] += 1
            stats["errors"].append(f"{file_path}: {str(e)}")
    
    return stats


def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description="Financial transaction database manager",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Import command
    import_parser = subparsers.add_parser(
        "import",
        aliases=["imp"],
        help="Import transactions from CSV files"
    )
    import_parser.add_argument(
        "--file",
        "-f",
        dest="files",
        action="append",
        required=True,
        help="Path to CSV file(s) to import (can be specified multiple times)"
    )
    import_parser.add_argument(
        "--account",
        type=str,
        help="Account name to link imported transactions to"
    )
    import_parser.add_argument(
        "--account-type",
        type=str,
        choices=["bank", "credit", "investment", "savings", "cash", "other"],
        help="Account type for new account creation or auto-detection"
    )
    import_parser.add_argument(
        "--no-categorize",
        action="store_true",
        help="Disable automatic categorization for this import"
    )
    import_parser.add_argument(
        "--wealthfront",
        action="store_true",
        help="Use Wealthfront-specific import (detects transfers, prompts for investment balance)"
    )
    
    # View command
    view_parser = subparsers.add_parser(
        "view",
        help="View transactions from the database"
    )
    view_parser.add_argument(
        "--ui",
        action="store_true",
        help="Launch Streamlit web UI instead of CLI"
    )
    # Add all CLI viewer arguments
    view_parser.add_argument("--date-start", type=str, help="Start date (YYYY-MM-DD)")
    view_parser.add_argument("--date-end", type=str, help="End date (YYYY-MM-DD)")
    view_parser.add_argument("--amount-min", type=float, help="Minimum amount")
    view_parser.add_argument("--amount-max", type=float, help="Maximum amount")
    view_parser.add_argument("--description", type=str, help="Search keywords in description")
    view_parser.add_argument("--category", type=str, help="Filter by category")
    view_parser.add_argument("--source-file", type=str, help="Filter by source file")
    view_parser.add_argument("--account-id", type=int, help="Filter by account ID")
    view_parser.add_argument("--account-name", type=str, help="Filter by account name")
    view_parser.add_argument("--limit", type=int, help="Maximum number of transactions")
    view_parser.add_argument("--offset", type=int, default=0, help="Number of transactions to skip")
    view_parser.add_argument(
        "--sort-by",
        type=str,
        default="date",
        choices=["id", "date", "description", "amount", "category", "source_file"],
        help="Column to sort by"
    )
    view_parser.add_argument("--sort-asc", action="store_true", help="Sort ascending")
    view_parser.add_argument("--export", type=str, metavar="FILE", help="Export to CSV file")
    view_parser.add_argument("--stats", action="store_true", help="Show summary statistics")
    
    # Account command
    account_parser = subparsers.add_parser(
        "account",
        aliases=["acc"],
        help="Manage financial accounts"
    )
    account_subparsers = account_parser.add_subparsers(dest="account_action", help="Account actions")
    
    # Account create
    acc_create = account_subparsers.add_parser("create", help="Create a new account")
    acc_create.add_argument("--name", type=str, required=True, help="Account name")
    acc_create.add_argument("--type", type=str, required=True, choices=["bank", "credit", "investment", "savings", "cash", "other"], help="Account type")
    acc_create.add_argument("--balance", type=float, default=0.0, help="Initial balance")
    
    # Account list
    account_subparsers.add_parser("list", help="List all accounts")
    
    # Account show
    acc_show = account_subparsers.add_parser("show", help="Show account details")
    acc_show.add_argument("--id", type=int, help="Account ID")
    acc_show.add_argument("--name", type=str, help="Account name")
    
    # Account update
    acc_update = account_subparsers.add_parser("update", help="Update an account")
    acc_update.add_argument("--id", type=int, required=True, help="Account ID")
    acc_update.add_argument("--name", type=str, help="New account name")
    acc_update.add_argument("--type", type=str, choices=["bank", "credit", "investment", "savings", "cash", "other"], help="New account type")
    acc_update.add_argument("--balance", type=float, help="New balance")
    
    # Account delete
    acc_delete = account_subparsers.add_parser("delete", help="Delete an account")
    acc_delete.add_argument("--id", type=int, required=True, help="Account ID")
    
    # Account recalculate
    acc_recalc = account_subparsers.add_parser("recalculate", help="Recalculate account balance")
    acc_recalc.add_argument("--id", type=int, required=True, help="Account ID")
    
    # Budget command
    budget_parser = subparsers.add_parser(
        "budget",
        aliases=["bud"],
        help="Manage budgets"
    )
    budget_subparsers = budget_parser.add_subparsers(dest="budget_action", help="Budget actions")
    
    # Budget create
    bud_create = budget_subparsers.add_parser("create", help="Create a budget")
    bud_create.add_argument("--category", type=str, required=True, help="Category name")
    bud_create.add_argument("--amount", type=float, required=True, help="Allocated amount")
    bud_create.add_argument("--start", type=str, required=True, help="Period start date (YYYY-MM-DD)")
    bud_create.add_argument("--end", type=str, required=True, help="Period end date (YYYY-MM-DD)")
    
    # Budget list
    budget_subparsers.add_parser("list", help="List all budgets")
    
    # Budget status
    bud_status = budget_subparsers.add_parser("status", help="Show budget status")
    bud_status.add_argument("--category", type=str, help="Category name (optional, shows all if not provided)")
    
    # Budget update
    bud_update = budget_subparsers.add_parser("update", help="Update a budget")
    bud_update.add_argument("--id", type=int, required=True, help="Budget ID")
    bud_update.add_argument("--amount", type=float, help="New allocated amount")
    bud_update.add_argument("--start", type=str, help="New period start date (YYYY-MM-DD)")
    bud_update.add_argument("--end", type=str, help="New period end date (YYYY-MM-DD)")
    
    # Budget delete
    bud_delete = budget_subparsers.add_parser("delete", help="Delete a budget")
    bud_delete.add_argument("--id", type=int, required=True, help="Budget ID")
    
    # Analyze command
    analyze_parser = subparsers.add_parser(
        "analyze",
        aliases=["report", "stats"],
        help="Generate analytical reports and visualizations"
    )
    analyze_parser.add_argument(
        "--ui",
        action="store_true",
        help="Launch Streamlit dashboard instead of CLI report"
    )
    analyze_parser.add_argument(
        "--report-type",
        type=str,
        choices=["summary", "categories", "trends", "accounts", "comparison", "full"],
        default="full",
        help="Type of report to generate (default: full)"
    )
    analyze_parser.add_argument(
        "--time-frame",
        type=str,
        default="6m",
        help="Time frame for analysis (e.g., '1m', '3m', '6m', '12m', 'all', or 'YYYY-MM-DD:YYYY-MM-DD')"
    )
    analyze_parser.add_argument(
        "--account-id",
        type=int,
        help="Filter by account ID"
    )
    analyze_parser.add_argument(
        "--account-type",
        type=str,
        choices=["bank", "credit", "investment", "savings", "cash", "other"],
        help="Filter by account type"
    )
    analyze_parser.add_argument(
        "--top-n",
        type=int,
        help="Limit to top N categories"
    )
    analyze_parser.add_argument(
        "--export",
        type=str,
        metavar="FILE",
        help="Export report to text file"
    )
    analyze_parser.add_argument(
        "--export-csv",
        type=str,
        metavar="FILE",
        help="Export data to CSV file"
    )
    analyze_parser.add_argument(
        "--export-chart",
        type=str,
        metavar="FILE",
        help="Export chart to image file (PNG)"
    )
    analyze_parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for full report export"
    )
    analyze_parser.add_argument(
        "--periods",
        type=str,
        default="1m,3m,6m,12m",
        help="Comma-separated periods for comparison (e.g., '1m,3m,6m')"
    )
    
    # Update-balance command
    update_parser = subparsers.add_parser(
        "update-balance",
        aliases=["balance"],
        help="Manually update account balance (for investment/savings accounts)"
    )
    update_parser.add_argument(
        "--account",
        type=str,
        required=True,
        help="Account name to update"
    )
    update_parser.add_argument(
        "--balance",
        type=float,
        help="New balance value (required unless --history is used)"
    )
    update_parser.add_argument(
        "--notes",
        type=str,
        help="Optional notes about this balance update"
    )
    update_parser.add_argument(
        "--history",
        action="store_true",
        help="Show balance history instead of updating"
    )
    update_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of history entries to show (default: 10)"
    )
    
    # Balance-override command
    override_parser = subparsers.add_parser(
        "balance-override",
        aliases=["override"],
        help="Manage balance overrides for accounts with incomplete historical data"
    )
    override_subparsers = override_parser.add_subparsers(dest="override_action", help="Override actions")
    
    # Set override
    override_set = override_subparsers.add_parser("set", help="Set a balance override")
    override_set.add_argument("--account", type=str, required=True, help="Account name")
    override_set.add_argument("--date", type=str, required=True, help="Override date (YYYY-MM-DD)")
    override_set.add_argument("--balance", type=float, required=True, help="Known balance as of date")
    override_set.add_argument("--notes", type=str, help="Optional notes")
    
    # List overrides
    override_list = override_subparsers.add_parser("list", help="List all overrides for an account")
    override_list.add_argument("--account", type=str, required=True, help="Account name")
    
    # Delete override
    override_delete = override_subparsers.add_parser("delete", help="Delete an override")
    override_delete.add_argument("--id", type=int, required=True, help="Override ID to delete")
    
    # Compare balances
    override_compare = override_subparsers.add_parser("compare", help="Compare balance with/without overrides")
    override_compare.add_argument("--account", type=str, required=True, help="Account name")
    
    # Reclassify-transfers command
    reclassify_parser = subparsers.add_parser(
        "reclassify-transfers",
        aliases=["reclassify", "detect-transfers"],
        help="Detect and reclassify internal transfers in existing transactions"
    )
    reclassify_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without making changes"
    )
    reclassify_parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    reclassify_parser.add_argument(
        "--stats",
        action="store_true",
        help="Show transfer statistics after reclassification"
    )
    
    args = parser.parse_args()
    
    # Handle case where no command is provided
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Load configuration
    config_path = Path(args.config)
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Ensure data directory exists before logging/database work
    try:
        ensure_data_dir(config)
    except Exception as exc:
        print(f"Failed to prepare data directory: {exc}", file=sys.stderr)
        sys.exit(1)
    
    # Setup logging
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    # Get database connection string
    try:
        connection_string = create_connection_string(config)
    except Exception as e:
        logger.error(f"Failed to create connection string: {e}")
        sys.exit(1)
    
    # Route to appropriate command handler
    if args.command in ["import", "imp"]:
        handle_import_command(args, config, connection_string)
    elif args.command == "view":
        handle_view_command(args, connection_string)
    elif args.command in ["account", "acc"]:
        handle_account_command(args, connection_string)
    elif args.command in ["budget", "bud"]:
        handle_budget_command(args, connection_string)
    elif args.command in ["analyze", "report", "stats"]:
        handle_analyze_command(args, connection_string)
    elif args.command in ["update-balance", "balance"]:
        handle_update_balance_command(args, connection_string)
    elif args.command in ["balance-override", "override"]:
        handle_balance_override_command(args, connection_string)
    elif args.command in ["reclassify-transfers", "reclassify", "detect-transfers"]:
        handle_reclassify_transfers_command(args, connection_string)
    else:
        parser.print_help()
        sys.exit(1)


def handle_import_command(args: argparse.Namespace, config: dict, connection_string: str) -> None:
    """
    Handle the import command.
    
    Args:
        args: Parsed command-line arguments
        config: Configuration dictionary
        connection_string: Database connection string
    """
    # Validate file paths
    file_paths = [Path(f) for f in args.files]
    invalid_files = [f for f in file_paths if not f.exists()]
    if invalid_files:
        logger.error(f"Files not found: {invalid_files}")
        sys.exit(1)
    
    # Initialize database
    try:
        db_manager = DatabaseManager(connection_string)
        db_manager.create_tables()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        sys.exit(1)
    
    # Import transactions (with account linking if enhanced import available)
    try:
        # Try enhanced import with account linking
        try:
            from enhanced_import import EnhancedImporter
            from account_management import AccountManager
            from categorization import CategorizationEngine
            from database_ops import AccountType
            
            account_manager = AccountManager(db_manager)
            categorization_engine = CategorizationEngine()
            categorization_engine.load_default_rules()
            
            enhanced_importer = EnhancedImporter(
                db_manager=db_manager,
                account_manager=account_manager,
                categorization_engine=categorization_engine if not args.no_categorize else None
            )
            
            # Import each file with account linking
            total_stats = {
                "files_processed": 0,
                "files_failed": 0,
                "total_rows_read": 0,
                "total_transactions_standardized": 0,
                "total_inserted": 0,
                "total_duplicates_found": 0,
                "total_skipped": 0,
                "errors": []
            }
            
            for file_path in file_paths:
                try:
                    account_type = None
                    if args.account_type:
                        account_type = AccountType[args.account_type.upper()]
                    
                    # Use Wealthfront-specific import if flag is set
                    if args.wealthfront:
                        result = enhanced_importer.import_wealthfront_cash(
                            file_path=file_path,
                            config=config,
                            prompt_investment_update=True
                        )
                    else:
                        result = enhanced_importer.import_with_account(
                            file_path=file_path,
                            account_name=args.account,
                            account_type=account_type,
                            config=config,
                            apply_categorization=not args.no_categorize
                        )
                    
                    if result.get("success"):
                        total_stats["files_processed"] += 1
                        total_stats["total_inserted"] += result.get("transactions_imported", 0)
                        total_stats["total_duplicates_found"] += result.get("duplicates_found", 0)
                        logger.info(
                            f"Imported {result.get('transactions_imported', 0)} transactions "
                            f"to account '{result.get('account_name')}'"
                        )
                    else:
                        total_stats["files_failed"] += 1
                        total_stats["errors"].append(result.get("error", "Unknown error"))
                except Exception as e:
                    logger.error(f"Error importing {file_path}: {e}", exc_info=True)
                    total_stats["files_failed"] += 1
                    total_stats["errors"].append(f"{file_path}: {str(e)}")
            
            stats = total_stats
        except ImportError:
            # Fall back to basic import
            logger.info("Enhanced import not available, using basic import")
            stats = import_transactions(file_paths, config, db_manager)
        
        # Print summary
        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        print(f"Files processed: {stats['files_processed']}")
        print(f"Files failed: {stats['files_failed']}")
        if 'total_rows_read' in stats:
            print(f"Total rows read: {stats['total_rows_read']}")
        if 'total_transactions_standardized' in stats:
            print(f"Total transactions standardized: {stats['total_transactions_standardized']}")
        print(f"Total duplicates found: {stats['total_duplicates_found']}")
        print(f"Total inserted: {stats['total_inserted']}")
        if 'total_skipped' in stats:
            print(f"Total skipped: {stats['total_skipped']}")
        
        if stats['errors']:
            print(f"\nErrors ({len(stats['errors'])}):")
            for error in stats['errors']:
                print(f"  - {error}")
        
        print("="*60)
        
        # Get final database count
        total_in_db = db_manager.get_transaction_count()
        print(f"Total transactions in database: {total_in_db}")
        print("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Import failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db_manager.close()


def handle_view_command(args: argparse.Namespace, connection_string: str) -> None:
    """
    Handle the view command.
    
    Args:
        args: Parsed command-line arguments
        connection_string: Database connection string
    """
    if args.ui:
        # Launch Streamlit UI
        import subprocess
        import os
        
        # Get the path to ui_viewer.py
        ui_viewer_path = os.path.join(os.path.dirname(__file__), "ui_viewer.py")
        
        # Set environment variable for connection string
        os.environ["DB_CONNECTION_STRING"] = connection_string
        
        # Modify sys.argv to pass connection string to Streamlit
        # We'll need to modify ui_viewer.py to read from environment or use a different approach
        # For now, we'll pass it via a temporary config file or environment variable
        
        # Create a wrapper script or modify ui_viewer to accept connection string
        # Simplest: modify ui_viewer to read from config or environment
        try:
            # Set environment variable for connection string
            os.environ["DB_CONNECTION_STRING"] = connection_string
            
            # Run streamlit
            subprocess.run([
                sys.executable, "-m", "streamlit", "run", ui_viewer_path
            ])
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Error launching Streamlit UI: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        # Use CLI viewer
        from cli_viewer import main_cli_viewer
        main_cli_viewer(connection_string, args)


def handle_account_command(args: argparse.Namespace, connection_string: str) -> None:
    """
    Handle account management commands.
    
    Args:
        args: Parsed command-line arguments
        connection_string: Database connection string
    """
    from account_management import AccountManager
    from database_ops import DatabaseManager, AccountType
    
    db_manager = DatabaseManager(connection_string)
    db_manager.create_tables()
    account_manager = AccountManager(db_manager)
    
    try:
        if args.account_action == "create":
            account_type = AccountType[args.type.upper()]
            account = account_manager.create_account(
                name=args.name,
                account_type=account_type,
                initial_balance=args.balance
            )
            if account:
                print(f"Created account: {account.name} ({account.type.value}) with balance ${args.balance:.2f}")
            else:
                print("Failed to create account", file=sys.stderr)
                sys.exit(1)
        
        elif args.account_action == "list":
            accounts = account_manager.list_accounts()
            if not accounts:
                print("No accounts found.")
            else:
                # Get signed balances and categorize
                assets = []
                liabilities = []
                
                for acc in accounts:
                    # Use signed balance (negative for credit accounts)
                    signed_balance = account_manager.get_signed_balance(acc.id)
                    
                    account_data = {
                        'id': acc.id,
                        'name': acc.name,
                        'type': acc.type.value,
                        'balance': signed_balance
                    }
                    
                    # Categorize based on signed balance
                    if signed_balance >= 0:
                        assets.append(account_data)
                    else:
                        liabilities.append(account_data)
                
                # Sort: assets by balance descending, liabilities by balance ascending (least negative first)
                assets.sort(key=lambda x: x['balance'], reverse=True)
                liabilities.sort(key=lambda x: x['balance'], reverse=False)
                
                # Display Assets
                if assets:
                    print("\n" + "=" * 95)
                    print("ASSETS")
                    print("=" * 95)
                    print(f"{'ID':<5} {'Name':<35} {'Type':<20} {'Balance':>20}")
                    print("-" * 95)
                    assets_total = 0.0
                    for acc in assets:
                        print(f"{acc['id']:<5} {acc['name']:<35} {acc['type']:<20} ${acc['balance']:>19,.2f}")
                        assets_total += acc['balance']
                    print("-" * 95)
                    print(f"{'TOTAL ASSETS':<60} ${assets_total:>19,.2f}")
                    print("=" * 95)
                
                # Display Liabilities
                if liabilities:
                    print("\n" + "=" * 95)
                    print("LIABILITIES")
                    print("=" * 95)
                    print(f"{'ID':<5} {'Name':<35} {'Type':<20} {'Balance':>20}")
                    print("-" * 95)
                    liabilities_total = 0.0
                    for acc in liabilities:
                        print(f"{acc['id']:<5} {acc['name']:<35} {acc['type']:<20} ${acc['balance']:>19,.2f}")
                        liabilities_total += acc['balance']
                    print("-" * 95)
                    print(f"{'TOTAL LIABILITIES':<60} ${liabilities_total:>19,.2f}")
                    print("=" * 95)
                
                # Display Net Worth
                net_worth = sum(acc['balance'] for acc in assets) + sum(acc['balance'] for acc in liabilities)
                print("\n" + "=" * 95)
                print(f"{'NET WORTH':<60} ${net_worth:>19,.2f}")
                print("=" * 95)
                print()
        
        elif args.account_action == "show":
            if args.id:
                account = account_manager.get_account(args.id)
            elif args.name:
                account = account_manager.get_account_by_name(args.name)
            else:
                print("Error: Must provide --id or --name", file=sys.stderr)
                sys.exit(1)
            
            if not account:
                print("Account not found", file=sys.stderr)
                sys.exit(1)
            
            summary = account_manager.get_account_summary(account.id)
            if summary:
                print("\n" + "=" * 60)
                print(f"ACCOUNT: {summary['name']}")
                print("=" * 60)
                print(f"ID: {summary['id']}")
                print(f"Type: {summary['type']}")
                print(f"Stored Balance: ${summary['stored_balance']:,.2f}")
                print(f"Calculated Balance: ${summary['calculated_balance']:,.2f}")
                print(f"Total Transactions: {summary['total_transactions']}")
                print(f"Recent Transactions (30 days): {summary['recent_transactions']}")
                print("=" * 60)
        
        elif args.account_action == "update":
            account_type = None
            if args.type:
                account_type = AccountType[args.type.upper()]
            
            account = account_manager.update_account(
                account_id=args.id,
                name=args.name,
                account_type=account_type,
                balance=args.balance
            )
            if account:
                print(f"Updated account {args.id}")
            else:
                print("Failed to update account", file=sys.stderr)
                sys.exit(1)
        
        elif args.account_action == "delete":
            if account_manager.delete_account(args.id):
                print(f"Deleted account {args.id}")
            else:
                print("Failed to delete account", file=sys.stderr)
                sys.exit(1)
        
        elif args.account_action == "recalculate":
            balance = account_manager.recalculate_balance(args.id)
            if balance is not None:
                print(f"Recalculated balance for account {args.id}: ${balance:,.2f}")
            else:
                print("Account not found", file=sys.stderr)
                sys.exit(1)
        
        else:
            print("Invalid account action", file=sys.stderr)
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Account command failed: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db_manager.close()


def handle_budget_command(args: argparse.Namespace, connection_string: str) -> None:
    """
    Handle budget management commands.
    
    Args:
        args: Parsed command-line arguments
        connection_string: Database connection string
    """
    from budgeting import BudgetManager
    from database_ops import DatabaseManager
    from datetime import datetime
    
    db_manager = DatabaseManager(connection_string)
    db_manager.create_tables()
    budget_manager = BudgetManager(db_manager)
    
    try:
        if args.budget_action == "create":
            start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
            end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
            
            budget = budget_manager.create_budget(
                category=args.category,
                allocated_amount=args.amount,
                period_start=start_date,
                period_end=end_date
            )
            if budget:
                print(f"Created budget for '{args.category}': ${args.amount:.2f} ({args.start} to {args.end})")
            else:
                print("Failed to create budget", file=sys.stderr)
                sys.exit(1)
        
        elif args.budget_action == "list":
            budgets = budget_manager.get_all_budgets()
            if not budgets:
                print("No active budgets found.")
            else:
                print("\n" + "=" * 100)
                print("BUDGETS")
                print("=" * 100)
                print(f"{'ID':<5} {'Category':<25} {'Allocated':>15} {'Period':<30}")
                print("-" * 100)
                for bud in budgets:
                    period = f"{bud.period_start.date()} to {bud.period_end.date()}"
                    print(f"{bud.id:<5} {bud.category:<25} ${bud.allocated_amount:>14,.2f} {period}")
                print("=" * 100)
        
        elif args.budget_action == "status":
            if args.category:
                status = budget_manager.get_budget_status(args.category)
                if not status:
                    print(f"No budget found for category '{args.category}'")
                else:
                    print("\n" + "=" * 60)
                    print(f"BUDGET STATUS: {status.category}")
                    print("=" * 60)
                    print(f"Allocated: ${status.allocated:,.2f}")
                    print(f"Spent: ${status.spent:,.2f}")
                    print(f"Remaining: ${status.remaining:,.2f}")
                    print(f"Percentage Used: {status.percentage_used:.1f}%")
                    print("=" * 60)
            else:
                statuses = budget_manager.get_all_budget_statuses()
                if not statuses:
                    print("No active budgets found.")
                else:
                    print("\n" + "=" * 100)
                    print("BUDGET STATUS")
                    print("=" * 100)
                    print(f"{'Category':<25} {'Allocated':>15} {'Spent':>15} {'Remaining':>15} {'Used %':>10}")
                    print("-" * 100)
                    for status in statuses:
                        print(f"{status.category:<25} ${status.allocated:>14,.2f} ${status.spent:>14,.2f} ${status.remaining:>14,.2f} {status.percentage_used:>9.1f}%")
                    print("=" * 100)
                    total_allocated = budget_manager.get_total_allocated()
                    total_spent = budget_manager.get_total_spent()
                    print(f"\nTotal Allocated: ${total_allocated:,.2f}")
                    print(f"Total Spent: ${total_spent:,.2f}")
                    print(f"Total Remaining: ${total_allocated - total_spent:,.2f}")
        
        elif args.budget_action == "update":
            start_date = None
            end_date = None
            if args.start:
                start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
            if args.end:
                end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
            
            budget = budget_manager.update_budget(
                budget_id=args.id,
                allocated_amount=args.amount,
                period_start=start_date,
                period_end=end_date
            )
            if budget:
                print(f"Updated budget {args.id}")
            else:
                print("Failed to update budget", file=sys.stderr)
                sys.exit(1)
        
        elif args.budget_action == "delete":
            if budget_manager.delete_budget(args.id):
                print(f"Deleted budget {args.id}")
            else:
                print("Failed to delete budget", file=sys.stderr)
                sys.exit(1)
        
        else:
            print("Invalid budget action", file=sys.stderr)
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Budget command failed: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db_manager.close()


def handle_analyze_command(args: argparse.Namespace, connection_string: str) -> None:
    """
    Handle the analyze command.
    
    Args:
        args: Parsed command-line arguments
        connection_string: Database connection string
    """
    # Check if UI mode
    if args.ui:
        # Launch Streamlit UI
        try:
            import subprocess
            import sys
            logger.info("Launching Streamlit analytics dashboard")
            subprocess.run([sys.executable, "-m", "streamlit", "run", "ui_analytics.py"])
        except Exception as e:
            logger.error(f"Failed to launch Streamlit UI: {e}")
            print(f"Error launching UI: {e}", file=sys.stderr)
            print("Make sure Streamlit is installed: pip install streamlit", file=sys.stderr)
            sys.exit(1)
    else:
        # Run CLI analytics
        try:
            from cli_analytics import main_cli_analytics
            logger.info(f"Running analytics report: {args.report_type}")
            main_cli_analytics(connection_string, args)
        except ImportError as e:
            logger.error(f"Failed to import analytics modules: {e}")
            print(f"Error: Analytics modules not found. {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Analytics command failed: {e}", exc_info=True)
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)


def handle_update_balance_command(args: argparse.Namespace, connection_string: str) -> None:
    """
    Handle the update-balance command.
    
    Args:
        args: Parsed command-line arguments
        connection_string: Database connection string
    """
    try:
        from manual_update import update_balance_cli, show_balance_history
        
        # Initialize database
        db_manager = DatabaseManager(connection_string)
        db_manager.create_tables()
        
        # Show history or update balance
        if args.history:
            show_balance_history(
                db_manager,
                args.account,
                limit=args.limit
            )
        else:
            # Validate balance is provided
            if args.balance is None:
                print("Error: --balance is required when not using --history", file=sys.stderr)
                sys.exit(1)
            
            # Update balance
            success = update_balance_cli(
                db_manager,
                args.account,
                args.balance,
                notes=args.notes
            )
            
            if not success:
                sys.exit(1)
        
    except ImportError as e:
        logger.error(f"Failed to import manual_update module: {e}")
        print(f"Error: Manual update module not found. {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Update balance command failed: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db_manager.close()


def handle_balance_override_command(args: argparse.Namespace, connection_string: str) -> None:
    """
    Handle the balance-override command.
    
    Args:
        args: Parsed command-line arguments
        connection_string: Database connection string
    """
    try:
        from balance_override_cli import (
            set_balance_override_cli,
            list_balance_overrides_cli,
            delete_balance_override_cli,
            show_balance_comparison_cli
        )
        
        # Initialize database
        db_manager = DatabaseManager(connection_string)
        db_manager.create_tables()
        
        # Route to appropriate action
        if args.override_action == "set":
            success = set_balance_override_cli(
                db_manager,
                args.account,
                args.date,
                args.balance,
                notes=args.notes
            )
            if not success:
                sys.exit(1)
        
        elif args.override_action == "list":
            list_balance_overrides_cli(db_manager, args.account)
        
        elif args.override_action == "delete":
            success = delete_balance_override_cli(db_manager, args.id)
            if not success:
                sys.exit(1)
        
        elif args.override_action == "compare":
            show_balance_comparison_cli(db_manager, args.account)
        
        else:
            print("Error: No action specified. Use 'set', 'list', 'delete', or 'compare'", file=sys.stderr)
            sys.exit(1)
        
    except ImportError as e:
        logger.error(f"Failed to import balance_override_cli module: {e}")
        print(f"Error: Balance override module not found. {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Balance override command failed: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db_manager.close()


def handle_reclassify_transfers_command(args: argparse.Namespace, connection_string: str) -> None:
    """
    Handle the reclassify-transfers command.
    
    Batch reclassify all transactions to detect internal transfers.
    
    Args:
        args: Parsed command-line arguments
        connection_string: Database connection string
    """
    from classification import batch_classify_transfers, get_transfer_statistics
    
    # Initialize database
    try:
        db_manager = DatabaseManager(connection_string)
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        print(f"Error: Failed to connect to database: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        print("=" * 70)
        print("Transfer Reclassification")
        print("=" * 70)
        print()
        
        if args.dry_run:
            print("DRY RUN MODE - No changes will be made")
            print()
        
        # Get statistics before reclassification
        if args.stats:
            print("Current Transfer Statistics:")
            stats_before = get_transfer_statistics(db_manager)
            print(f"  Total Transactions: {stats_before['total_transactions']}")
            print(f"  Marked as Transfers: {stats_before['total_transfers']} ({stats_before['transfer_percentage']:.1f}%)")
            print(f"  Transfer Amount Total: ${stats_before['transfer_amount_total']:,.2f}")
            print()
        
        # Run batch classification
        print("Scanning transactions for transfers...")
        print()
        
        result = batch_classify_transfers(
            db_manager=db_manager,
            config_path=args.config,
            dry_run=args.dry_run
        )
        
        # Display results
        print("Results:")
        print(f"  Total Transactions Scanned: {result['total']}")
        print(f"  Transfers Detected: {result['transfers_found']}")
        
        if args.dry_run:
            print(f"  Would Update: {result['transfers_found']} transactions")
        else:
            print(f"  Transactions Updated: {result['updated']}")
        
        if result['errors'] > 0:
            print(f"  Errors: {result['errors']}")
        
        print()
        
        # Get statistics after reclassification
        if args.stats and not args.dry_run:
            print("Updated Transfer Statistics:")
            stats_after = get_transfer_statistics(db_manager)
            print(f"  Total Transactions: {stats_after['total_transactions']}")
            print(f"  Marked as Transfers: {stats_after['total_transfers']} ({stats_after['transfer_percentage']:.1f}%)")
            print(f"  Transfer Amount Total: ${stats_after['transfer_amount_total']:,.2f}")
            print()
            
            # Show change
            if 'stats_before' in locals():
                change = stats_after['total_transfers'] - stats_before['total_transfers']
                if change > 0:
                    print(f"  [+] {change} additional transaction(s) marked as transfers")
                elif change < 0:
                    print(f"  [-] {abs(change)} fewer transaction(s) marked as transfers")
                else:
                    print("  [=] No change in transfer count")
                print()
        
        if not args.dry_run:
            print("[SUCCESS] Reclassification complete!")
            print()
            print("Note: Transfers are excluded from spending analytics by default.")
            print("      Use the 'Include Transfers' checkbox in the UI to include them.")
        else:
            print("To apply these changes, run without --dry-run flag")
        
        print("=" * 70)
        
    except Exception as e:
        logger.error(f"Reclassify transfers command failed: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db_manager.close()


if __name__ == "__main__":
    main()

