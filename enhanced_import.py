"""
Enhanced import module with account linking and transfer detection.

This module extends the basic import functionality to support:
- Linking transactions to accounts during import
- Detecting inter-account transfers
- Auto-detecting account types from CSV files
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import re

from data_ingestion import CSVReader
from data_standardization import DataStandardizer
from duplicate_detection import DuplicateDetector
from database_ops import DatabaseManager, Account, AccountType, Transaction
from account_management import AccountManager
from categorization import CategorizationEngine

# Configure logging
logger = logging.getLogger(__name__)


class EnhancedImporter:
    """
    Enhanced importer with account linking and transfer detection.
    
    Extends the basic import functionality to automatically link transactions
    to accounts and detect transfers between accounts.
    """
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        account_manager: AccountManager,
        categorization_engine: Optional[CategorizationEngine] = None
    ):
        """
        Initialize the enhanced importer.
        
        Args:
            db_manager: DatabaseManager instance
            account_manager: AccountManager instance
            categorization_engine: Optional categorization engine
        """
        self.db_manager = db_manager
        self.account_manager = account_manager
        self.categorization_engine = categorization_engine or CategorizationEngine()
        
        # Load default categorization rules if engine is empty
        if not self.categorization_engine.rules:
            self.categorization_engine.load_default_rules()
        
        logger.info("Enhanced importer initialized")
    
    def detect_account_type_from_filename(self, filename: str) -> Optional[AccountType]:
        """
        Attempt to detect account type from filename.
        
        Args:
            filename: CSV filename
        
        Returns:
            Detected AccountType or None
        """
        filename_lower = filename.lower()
        
        # Check for credit card indicators
        if any(keyword in filename_lower for keyword in ["credit", "card", "visa", "mastercard", "amex", "discover"]):
            return AccountType.CREDIT
        
        # Check for investment indicators
        if any(keyword in filename_lower for keyword in ["investment", "portfolio", "401k", "ira", "brokerage", "stocks"]):
            return AccountType.INVESTMENT
        
        # Check for bank indicators
        if any(keyword in filename_lower for keyword in ["checking", "savings", "bank", "account"]):
            return AccountType.BANK
        
        # Default to bank for most cases
        return AccountType.BANK
    
    def detect_account_type_from_headers(self, headers: List[str]) -> Optional[AccountType]:
        """
        Attempt to detect account type from CSV headers.
        
        Args:
            headers: List of CSV column headers
        
        Returns:
            Detected AccountType or None
        """
        headers_str = " ".join(headers).lower()
        
        # Credit card indicators
        if any(keyword in headers_str for keyword in ["credit", "card", "payment due", "interest", "apr"]):
            return AccountType.CREDIT
        
        # Investment indicators
        if any(keyword in headers_str for keyword in ["shares", "symbol", "dividend", "capital gain", "portfolio"]):
            return AccountType.INVESTMENT
        
        # Bank indicators (default)
        return AccountType.BANK
    
    def detect_or_create_account(
        self,
        account_name: Optional[str] = None,
        account_type: Optional[AccountType] = None,
        filename: Optional[str] = None,
        headers: Optional[List[str]] = None
    ) -> Optional[Account]:
        """
        Detect or create an account for import.
        
        Args:
            account_name: Explicit account name (if provided)
            account_type: Explicit account type (if provided)
            filename: CSV filename for auto-detection
            headers: CSV headers for auto-detection
        
        Returns:
            Account object or None if creation failed
        """
        # Determine account name
        if not account_name:
            if filename:
                # Extract account name from filename (remove extension and path)
                account_name = Path(filename).stem
                # Clean up the name
                account_name = re.sub(r'[_-]', ' ', account_name).title()
            else:
                account_name = "Imported Account"
        
        # Determine account type
        if not account_type:
            if filename:
                account_type = self.detect_account_type_from_filename(filename)
            elif headers:
                account_type = self.detect_account_type_from_headers(headers)
            else:
                account_type = AccountType.BANK  # Default
        
        # Check if account exists
        existing_account = self.account_manager.get_account_by_name(account_name)
        if existing_account:
            logger.info(f"Using existing account: {account_name}")
            return existing_account
        
        # Create new account
        logger.info(f"Creating new account: {account_name} ({account_type.value})")
        account = self.account_manager.create_account(
            name=account_name,
            account_type=account_type,
            initial_balance=0.0
        )
        
        return account
    
    def detect_transfer(
        self,
        transaction: Dict[str, Any],
        all_transactions: List[Dict[str, Any]]
    ) -> Optional[Tuple[int, int]]:
        """
        Detect if a transaction is a transfer between accounts.
        
        Args:
            transaction: Transaction dictionary to check
            all_transactions: List of all transactions being imported
        
        Returns:
            Tuple of (from_account_id, to_account_id) if transfer detected, None otherwise
        """
        description = transaction.get("description", "").upper()
        
        # Check for transfer keywords
        transfer_keywords = ["TRANSFER", "MOVE MONEY", "SEND MONEY", "RECEIVE MONEY"]
        if not any(keyword in description for keyword in transfer_keywords):
            return None
        
        # Look for matching transaction with opposite amount
        amount = transaction.get("amount", 0)
        date = transaction.get("date")
        
        if not date:
            return None
        
        # Find transactions with opposite amount on same or nearby date
        for other_trans in all_transactions:
            if other_trans == transaction:
                continue
            
            other_amount = other_trans.get("amount", 0)
            other_date = other_trans.get("date")
            
            # Check if amounts are opposite (within small tolerance)
            if abs(amount + other_amount) < 0.01 and other_date:
                # Check if dates are close (within 1 day)
                if isinstance(date, datetime) and isinstance(other_date, datetime):
                    date_diff = abs((date - other_date).days)
                    if date_diff <= 1:
                        # Potential transfer pair
                        from_account_id = transaction.get("account_id")
                        to_account_id = other_trans.get("account_id")
                        
                        if from_account_id and to_account_id and from_account_id != to_account_id:
                            return (from_account_id, to_account_id)
        
        return None
    
    def import_with_account(
        self,
        file_path: Path,
        account_name: Optional[str] = None,
        account_type: Optional[AccountType] = None,
        config: Dict[str, Any] = None,
        apply_categorization: bool = True
    ) -> Dict[str, Any]:
        """
        Import CSV file and link transactions to an account.
        
        Args:
            file_path: Path to CSV file
            account_name: Optional explicit account name
            account_type: Optional explicit account type
            config: Configuration dictionary
            apply_categorization: Whether to apply automatic categorization
        
        Returns:
            Dictionary with import statistics
        """
        from data_ingestion import CSVReader
        from data_standardization import DataStandardizer
        
        # Initialize components
        csv_reader = CSVReader(chunk_size=config.get("processing", {}).get("chunk_size", 10000) if config else 10000)
        
        standardizer = DataStandardizer(
            column_mappings=config.get("column_mappings", {}) if config else {},
            date_formats=config.get("processing", {}).get("date_formats", []) if config else [],
            output_date_format=config.get("processing", {}).get("output_date_format", "%Y-%m-%d") if config else "%Y-%m-%d",
            amount_decimal_places=config.get("processing", {}).get("amount_decimal_places", 2) if config else 2
        )
        
        duplicate_detector = DuplicateDetector(
            key_fields=config.get("duplicate_detection", {}).get("key_fields", ["date", "description", "amount"]) if config else ["date", "description", "amount"],
            hash_algorithm=config.get("duplicate_detection", {}).get("hash_algorithm", "md5") if config else "md5"
        )
        
        # Read and standardize CSV
        df = csv_reader.read_csv(file_path, chunked=False)
        standardized = standardizer.standardize_dataframe(df, source_file=str(file_path.name))
        
        # Detect or create account
        account = self.detect_or_create_account(
            account_name=account_name,
            account_type=account_type,
            filename=file_path.name,
            headers=df.columns.tolist()
        )
        
        if not account:
            logger.error("Failed to create or find account")
            return {
                "success": False,
                "error": "Failed to create or find account",
                "transactions_imported": 0
            }
        
        # CRITICAL: Invert sign for credit card transactions with inverted CSV format
        # Some credit card providers (e.g., Robinhood) use: purchases=positive, payments=negative
        # App internal format: purchases=negative (increase debt), payments=positive (reduce debt)
        # Note: Chase cards already use the correct format and don't need inversion
        invert_sign_accounts = ["Robinhood Gold Card"]  # List of accounts that need sign inversion
        
        if account.type == AccountType.CREDIT and account.name in invert_sign_accounts:
            logger.info(f"Inverting transaction signs for credit card account: {account.name}")
            for trans in standardized:
                if trans.get("amount") is not None:
                    trans["amount"] = -trans["amount"]
        
        # Apply categorization, transfer detection, and link to account
        from classification import is_transfer, is_credit_card_payment, load_transfer_patterns
        
        # Load transfer patterns once
        transfer_patterns = load_transfer_patterns()
        transfer_config = config.get("transfer_detection", {}) if config else {}
        transfer_category = transfer_config.get("transfer_category", "Transfer")
        
        for trans in standardized:
            # Apply automatic categorization
            if apply_categorization and not trans.get("category"):
                category = self.categorization_engine.categorize(
                    description=trans.get("description", ""),
                    amount=trans.get("amount"),
                    existing_category=trans.get("category")
                )
                if category:
                    trans["category"] = category
            
            # Detect transfers (pattern matching)
            is_transfer_match = is_transfer(trans.get("description", ""), transfer_patterns)
            
            # Additional safeguard: Check for credit card payments
            is_cc_payment = is_credit_card_payment(
                trans.get("description", ""),
                account_type=account.type if account else None,
                account_name=account.name if account else None
            )
            
            # Mark as transfer if either check passes
            if is_transfer_match or is_cc_payment:
                trans["is_transfer"] = 1
                # Optionally set transfer category
                if transfer_category and not trans.get("category"):
                    trans["category"] = transfer_category
                detection_method = "pattern" if is_transfer_match else "credit card safeguard"
                logger.debug(f"Transfer detected during import ({detection_method}): {trans.get('description')}")
            else:
                trans["is_transfer"] = 0
            
            # Link to account
            trans["account_id"] = account.id
            trans["account"] = account.name  # Legacy field
        
        # Generate hashes and check duplicates
        hashes = duplicate_detector.generate_hashes_batch(standardized)
        standardized_with_hashes = [
            {**trans, "duplicate_hash": hash_val}
            for trans, hash_val in zip(standardized, hashes)
            if hash_val is not None
        ]
        
        # Check for duplicates
        existing_hashes = set(
            self.db_manager.check_duplicate_hashes([t["duplicate_hash"] for t in standardized_with_hashes])
        )
        
        unique_transactions, duplicate_transactions = duplicate_detector.filter_duplicates(
            standardized_with_hashes,
            existing_hashes
        )
        
        # Detect transfers
        transfer_count = 0
        for trans in unique_transactions:
            transfer_info = self.detect_transfer(trans, unique_transactions)
            if transfer_info:
                trans["is_transfer"] = 1
                trans["transfer_to_account_id"] = transfer_info[1]
                transfer_count += 1
        
        # Insert transactions
        inserted, skipped = self.db_manager.insert_transactions(unique_transactions)
        
        # Recalculate account balance
        self.account_manager.recalculate_balance(account.id)
        
        return {
            "success": True,
            "account_id": account.id,
            "account_name": account.name,
            "transactions_imported": inserted,
            "duplicates_found": len(duplicate_transactions),
            "transfers_detected": transfer_count,
            "skipped": skipped
        }
    
    def import_wealthfront_cash(
        self,
        file_path: Path,
        config: dict,
        prompt_investment_update: bool = True
    ) -> dict:
        """
        Import Wealthfront Cash Savings CSV with transfer detection.
        
        Automatically detects transfers to investment account and optionally
        prompts user to update investment balance.
        
        Args:
            file_path: Path to Wealthfront Cash CSV file
            config: Configuration dictionary
            prompt_investment_update: Whether to prompt for investment balance update
        
        Returns:
            Dictionary with import results including transfer information
        """
        from manual_update import detect_wealthfront_transfers, prompt_balance_update_cli
        
        # Get Wealthfront config
        wf_config = config.get('wealthfront', {})
        cash_account_name = wf_config.get('cash_account_name', 'Wealthfront Cash Savings')
        investment_account_name = wf_config.get('investment_account_name', 'Wealthfront Automated Investment')
        
        # Ensure accounts exist
        cash_account = self.account_manager.get_or_create_account(
            cash_account_name,
            AccountType.SAVINGS,
            initial_balance=0.0
        )
        
        if not cash_account:
            return {
                "success": False,
                "error": "Failed to get/create Cash Savings account"
            }
        
        # Import transactions
        result = self.import_with_account(
            file_path=file_path,
            account_name=cash_account_name,
            account_type=AccountType.SAVINGS,
            config=config,
            apply_categorization=True
        )
        
        if not result["success"]:
            return result
        
        # Detect transfers to investment
        # Re-read the transactions we just imported (simpler than passing them through)
        from data_ingestion import CSVReader
        from data_standardization import DataStandardizer
        
        reader = CSVReader(config)
        df = reader.read_csv(file_path)
        
        if df is not None and not df.empty:
            # Initialize standardizer with config parameters
            processing_config = config.get('processing', {})
            standardizer = DataStandardizer(
                column_mappings=config.get('column_mappings', {}),
                date_formats=processing_config.get('date_formats', ['%Y-%m-%d', '%m/%d/%Y']),
                output_date_format=processing_config.get('output_date_format', '%Y-%m-%d'),
                amount_decimal_places=processing_config.get('amount_decimal_places', 2)
            )
            standardized = standardizer.standardize_dataframe(df, source_file=str(file_path.name))
            
            if standardized is not None and len(standardized) > 0:
                # standardize_dataframe returns a list of dicts, not a DataFrame
                transfers = detect_wealthfront_transfers(standardized, config)
                
                result['wealthfront_transfers'] = len(transfers)
                result['transfer_total'] = sum(abs(t.get('amount', 0)) for t in transfers)
                
                # Prompt for investment balance update if there are transfers
                if transfers and prompt_investment_update:
                    # Ensure investment account exists
                    investment_account = self.account_manager.get_or_create_account(
                        investment_account_name,
                        AccountType.INVESTMENT,
                        initial_balance=0.0
                    )
                    
                    if investment_account:
                        print(f"\n{len(transfers)} transfer(s) to investment detected (total: ${result['transfer_total']:.2f})")
                        # Get current balance from account object or dict
                        current_balance = investment_account.balance if hasattr(investment_account, 'balance') else investment_account.get('balance', 0.0)
                        prompt_balance_update_cli(
                            self.account_manager,
                            investment_account_name,
                            current_balance
                        )
        
        return result

