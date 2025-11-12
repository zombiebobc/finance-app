"""
Unit tests for financial transaction CSV importer.

Tests cover:
- CSV reading and parsing
- Data standardization
- Duplicate detection
- Database operations
"""

import pytest
import tempfile
import os
from io import BytesIO
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from account_management import AccountManager
from data_ingestion import CSVReader, preview_csv
from data_standardization import DataStandardizer
from duplicate_detection import DuplicateDetector
from enhanced_import import EnhancedImporter
from fix_robinhood_payments import fix_robinhood_transactions
from database_ops import AccountType, DatabaseManager, Transaction, Base
from utils import IngestionError


@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file with sample transaction data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("Date,Description,Amount,Category\n")
        f.write("2024-01-15,Grocery Store,-45.50,Groceries\n")
        f.write("2024-01-16,Gas Station,-30.00,Transportation\n")
        f.write("2024-01-17,Salary Deposit,2500.00,Income\n")
        temp_path = f.name
    
    yield Path(temp_path)
    
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def sample_csv_file_variant():
    """Create a CSV file with different column names."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("Transaction Date,Transaction Description,Transaction Amount,Type\n")
        f.write("01/15/2024,Store Purchase,45.50,Groceries\n")
        f.write("01/16/2024,Fuel Payment,30.00,Transportation\n")
        temp_path = f.name
    
    yield Path(temp_path)
    
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def test_database():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        temp_db = f.name
    
    connection_string = f"sqlite:///{temp_db}"
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)
    
    yield connection_string
    
    # Cleanup
    engine.dispose()
    if os.path.exists(temp_db):
        os.unlink(temp_db)


@pytest.fixture
def column_mappings():
    """Sample column mappings for testing."""
    return {
        "date": ["date", "transaction date", "transaction_date"],
        "description": ["description", "transaction description", "transaction_description"],
        "amount": ["amount", "transaction amount", "transaction_amount"],
        "category": ["category", "type", "transaction type"]
    }


@pytest.fixture
def date_formats():
    """Sample date formats for testing."""
    return [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y"
    ]


class TestCSVReader:
    """Tests for CSVReader class."""
    
    def test_read_csv(self, sample_csv_file):
        """Test reading a CSV file."""
        reader = CSVReader()
        df = reader.read_csv(sample_csv_file, chunked=False)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "Date" in df.columns
        assert "Description" in df.columns
        assert "Amount" in df.columns
    
    def test_validate_csv(self, sample_csv_file):
        """Test CSV validation."""
        reader = CSVReader()
        is_valid, error_msg = reader.validate_csv(sample_csv_file)
        
        assert is_valid is True
        assert error_msg is None
    
    def test_validate_csv_nonexistent(self):
        """Test validation of non-existent file."""
        reader = CSVReader()
        is_valid, error_msg = reader.validate_csv(Path("nonexistent.csv"))
        
        assert is_valid is False
        assert error_msg is not None
    
    def test_get_file_info(self, sample_csv_file):
        """Test getting file information."""
        reader = CSVReader()
        info = reader.get_file_info(sample_csv_file)
        
        assert info["rows"] == 3
        assert "Date" in info["columns"]
        assert info["valid"] is True
    
    def test_preview_csv(self, sample_csv_file):
        """Ensure preview_csv returns a limited DataFrame from bytes."""
        with open(sample_csv_file, "rb") as handle:
            preview = preview_csv(BytesIO(handle.read()))
        
        assert isinstance(preview, pd.DataFrame)
        assert not preview.empty
        assert len(preview) <= 10
    
    def test_preview_csv_malformed(self):
        """Malformed content should raise an IngestionError."""
        bad_bytes = BytesIO(b"\xff\xfe\x00\x01garbage")
        with pytest.raises(IngestionError):
            preview_csv(bad_bytes)


class TestDataStandardizer:
    """Tests for DataStandardizer class."""
    
    def test_map_columns(self, column_mappings):
        """Test column name mapping."""
        standardizer = DataStandardizer(column_mappings, [])
        
        csv_columns = ["Date", "Description", "Amount", "Category"]
        mapping = standardizer.map_columns(csv_columns)
        
        assert mapping["date"] == "Date"
        assert mapping["description"] == "Description"
        assert mapping["amount"] == "Amount"
        assert mapping["category"] == "Category"
    
    def test_map_columns_fuzzy(self, column_mappings):
        """Test fuzzy column name matching."""
        standardizer = DataStandardizer(column_mappings, [])
        
        csv_columns = ["Transaction Date", "Transaction Description", "Transaction Amount"]
        mapping = standardizer.map_columns(csv_columns)
        
        assert mapping["date"] == "Transaction Date"
        assert mapping["description"] == "Transaction Description"
        assert mapping["amount"] == "Transaction Amount"
    
    def test_parse_date(self, date_formats):
        """Test date parsing."""
        standardizer = DataStandardizer({}, date_formats)
        
        # Test ISO format
        date1 = standardizer._parse_date("2024-01-15")
        assert isinstance(date1, datetime)
        assert date1.year == 2024
        assert date1.month == 1
        assert date1.day == 15
        
        # Test US format
        date2 = standardizer._parse_date("01/15/2024")
        assert isinstance(date2, datetime)
        assert date2.year == 2024
    
    def test_parse_amount(self):
        """Test amount parsing."""
        standardizer = DataStandardizer({}, [])
        
        assert standardizer._parse_amount("45.50") == 45.50
        assert standardizer._parse_amount("-30.00") == -30.00
        assert standardizer._parse_amount("$100.00") == 100.00
        assert standardizer._parse_amount("1,000.50") == 1000.50
        assert standardizer._parse_amount(45.5) == 45.50
    
    def test_standardize_dataframe(self, sample_csv_file, column_mappings, date_formats):
        """Test standardizing a DataFrame."""
        reader = CSVReader()
        df = reader.read_csv(sample_csv_file, chunked=False)
        
        standardizer = DataStandardizer(column_mappings, date_formats)
        standardized = standardizer.standardize_dataframe(df, "test.csv")
        
        assert len(standardized) == 3
        assert all("date" in t for t in standardized)
        assert all("description" in t for t in standardized)
        assert all("amount" in t for t in standardized)
        assert all(isinstance(t["date"], datetime) for t in standardized)
        assert all(isinstance(t["amount"], float) for t in standardized)


class TestDuplicateDetector:
    """Tests for DuplicateDetector class."""
    
    def test_generate_hash(self):
        """Test hash generation."""
        detector = DuplicateDetector(["date", "description", "amount"])
        
        transaction = {
            "date": datetime(2024, 1, 15),
            "description": "Grocery Store",
            "amount": -45.50
        }
        
        hash1 = detector.generate_hash(transaction)
        hash2 = detector.generate_hash(transaction)
        
        # Same transaction should produce same hash
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hash length
    
    def test_generate_hash_different_transactions(self):
        """Test that different transactions produce different hashes."""
        detector = DuplicateDetector(["date", "description", "amount"])
        
        trans1 = {
            "date": datetime(2024, 1, 15),
            "description": "Grocery Store",
            "amount": -45.50
        }
        
        trans2 = {
            "date": datetime(2024, 1, 16),  # Different date
            "description": "Grocery Store",
            "amount": -45.50
        }
        
        hash1 = detector.generate_hash(trans1)
        hash2 = detector.generate_hash(trans2)
        
        assert hash1 != hash2
    
    def test_filter_duplicates(self, test_database):
        """Test filtering duplicates."""
        detector = DuplicateDetector(["date", "description", "amount"])
        db_manager = DatabaseManager(test_database)
        
        # Create a transaction in the database
        transaction1 = {
            "date": datetime(2024, 1, 15),
            "description": "Grocery Store",
            "amount": -45.50,
            "source_file": "test.csv",
            "duplicate_hash": detector.generate_hash({
                "date": datetime(2024, 1, 15),
                "description": "Grocery Store",
                "amount": -45.50
            })
        }
        db_manager.insert_transactions([transaction1])
        
        # Prepare new transactions (one duplicate, one unique)
        transactions = [
            {
                "date": datetime(2024, 1, 15),
                "description": "Grocery Store",
                "amount": -45.50
            },
            {
                "date": datetime(2024, 1, 16),
                "description": "Gas Station",
                "amount": -30.00
            }
        ]
        
        # Get existing hashes
        hashes = [detector.generate_hash(t) for t in transactions]
        existing_hashes = set(db_manager.check_duplicate_hashes(hashes))
        
        # Filter duplicates
        unique, duplicates = detector.filter_duplicates(transactions, existing_hashes)
        
        assert len(unique) == 1
        assert len(duplicates) == 1
        assert unique[0]["description"] == "Gas Station"
        assert duplicates[0]["description"] == "Grocery Store"
        
        db_manager.close()


class TestDatabaseManager:
    """Tests for DatabaseManager class."""
    
    def test_create_tables(self, test_database):
        """Test table creation."""
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        
        # Verify table exists by querying
        session = db_manager.get_session()
        try:
            count = session.query(Transaction).count()
            assert count == 0  # Table exists but is empty
        finally:
            session.close()
        
        db_manager.close()
    
    def test_insert_transactions(self, test_database):
        """Test inserting transactions."""
        detector = DuplicateDetector(["date", "description", "amount"])
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        
        transactions = [
            {
                "date": datetime(2024, 1, 15),
                "description": "Grocery Store",
                "amount": -45.50,
                "category": "Groceries",
                "source_file": "test.csv",
                "duplicate_hash": detector.generate_hash({
                    "date": datetime(2024, 1, 15),
                    "description": "Grocery Store",
                    "amount": -45.50
                })
            },
            {
                "date": datetime(2024, 1, 16),
                "description": "Gas Station",
                "amount": -30.00,
                "source_file": "test.csv",
                "duplicate_hash": detector.generate_hash({
                    "date": datetime(2024, 1, 16),
                    "description": "Gas Station",
                    "amount": -30.00
                })
            }
        ]
        
        inserted, skipped = db_manager.insert_transactions(transactions)
        
        assert inserted == 2
        assert skipped == 0
        
        # Verify in database
        count = db_manager.get_transaction_count()
        assert count == 2
        
        db_manager.close()
    
    def test_check_duplicate_hashes(self, test_database):
        """Test checking for duplicate hashes."""
        detector = DuplicateDetector(["date", "description", "amount"])
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        
        # Insert a transaction
        transaction = {
            "date": datetime(2024, 1, 15),
            "description": "Grocery Store",
            "amount": -45.50,
            "source_file": "test.csv",
            "duplicate_hash": detector.generate_hash({
                "date": datetime(2024, 1, 15),
                "description": "Grocery Store",
                "amount": -45.50
            })
        }
        db_manager.insert_transactions([transaction])
        
        # Check for duplicates
        hash_to_check = detector.generate_hash({
            "date": datetime(2024, 1, 15),
            "description": "Grocery Store",
            "amount": -45.50
        })
        
        existing = db_manager.check_duplicate_hashes([hash_to_check])
        assert hash_to_check in existing
        
        # Check for non-duplicate
        new_hash = detector.generate_hash({
            "date": datetime(2024, 1, 16),
            "description": "Gas Station",
            "amount": -30.00
        })
        
        existing = db_manager.check_duplicate_hashes([new_hash])
        assert new_hash not in existing
        
        db_manager.close()


class TestIntegration:
    """Integration tests for the full import process."""
    
    def test_end_to_end_import(self, sample_csv_file, test_database, column_mappings, date_formats):
        """Test the complete import process from CSV to database."""
        # Read CSV
        reader = CSVReader()
        df = reader.read_csv(sample_csv_file, chunked=False)
        
        # Standardize
        standardizer = DataStandardizer(column_mappings, date_formats)
        standardized = standardizer.standardize_dataframe(df, sample_csv_file.name)
        
        # Generate hashes and check duplicates
        detector = DuplicateDetector(["date", "description", "amount"])
        hashes = detector.generate_hashes_batch(standardized)
        standardized_with_hashes = [
            {**trans, "duplicate_hash": hash_val}
            for trans, hash_val in zip(standardized, hashes)
            if hash_val is not None
        ]
        
        # Insert into database
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        
        existing_hashes = set(
            db_manager.check_duplicate_hashes([t["duplicate_hash"] for t in standardized_with_hashes])
        )
        
        unique_transactions, _ = detector.filter_duplicates(
            standardized_with_hashes,
            existing_hashes
        )
        
        inserted, skipped = db_manager.insert_transactions(unique_transactions)
        
        assert inserted == 3
        assert skipped == 0
        
        # Verify database
        count = db_manager.get_transaction_count()
        assert count == 3
        
        db_manager.close()


class TestEnhancedImporter:
    """Tests for enhanced importer helpers."""
    
    def test_account_suggestions(self, test_database):
        """Account suggestions should include existing names and create-new sentinel."""
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        account_manager = AccountManager(db_manager)
        account_manager.create_account("Chase Checking", AccountType.BANK)
        
        suggestions = account_manager.get_account_suggestions("chase_checking.csv")
        
        assert "Chase Checking" in suggestions
        assert suggestions[-1] == "Create New Account"
        
        db_manager.close()
    
    def test_account_suggestions_empty_db(self, test_database):
        """When no accounts exist, only Create New Account should appear."""
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        account_manager = AccountManager(db_manager)
        
        suggestions = account_manager.get_account_suggestions("mysterious_file.csv")
        
        assert suggestions[-1] == "Create New Account"
        db_manager.close()
    
    def test_batch_import_creates_new_account_with_override(
        self,
        test_database,
        column_mappings,
        date_formats
    ):
        """Batch import should create a new account and respect initial balance overrides."""
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        account_manager = AccountManager(db_manager)
        importer = EnhancedImporter(db_manager, account_manager)
        
        csv_content = "Date,Description,Amount\n2024-01-01,Initial Deposit,1000.00\n"
        file_bytes = BytesIO(csv_content.encode("utf-8"))
        
        config = {
            "column_mappings": column_mappings,
            "processing": {
                "date_formats": date_formats,
                "output_date_format": "%Y-%m-%d",
            },
            "duplicate_detection": {
                "key_fields": ["date", "description", "amount"],
                "hash_algorithm": "md5",
            },
        }
        
        result = importer.batch_import(
            [
                {
                    "file_obj": file_bytes,
                    "filename": "test_account.csv",
                    "new_account": {
                        "name": "Test Account",
                        "type": "bank",
                        "initial_balance": 500.0,
                    },
                    "debug": {"fake": True},
                }
            ],
            config=config
        )
        
        assert result["success"] is True
        details = result["details"][0]
        assert details["imported"] == 1
        
        account = account_manager.get_account_by_name("Test Account")
        assert account is not None
        
        overrides = account_manager.get_balance_overrides(account.id)
        assert overrides, "Expected a balance override for the new account"
        assert db_manager.get_transaction_count() == 1
        assert result["details"][0]["debug"]["input_debug"] == {"fake": True}
        
        db_manager.close()

    def test_batch_import_inverts_robinhood_signs(
        self,
        test_database,
        column_mappings,
        date_formats
    ):
        """Robinhood Gold Card purchases should be stored as negative amounts."""
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        account_manager = AccountManager(db_manager)
        importer = EnhancedImporter(db_manager, account_manager)

        account = account_manager.create_account("Robinhood Gold Card", AccountType.CREDIT)
        assert account is not None

        csv_content = (
            "Date,Description,Amount\n"
            "2024-01-02,Purchase,150.00\n"
            "2024-01-03,Payment Thank You,-75.00\n"
        )
        file_bytes = BytesIO(csv_content.encode("utf-8"))

        config = {
            "column_mappings": column_mappings,
            "processing": {
                "date_formats": date_formats,
                "output_date_format": "%Y-%m-%d",
            },
            "duplicate_detection": {
                "key_fields": ["date", "description", "amount"],
                "hash_algorithm": "md5",
            },
        }

        try:
            result = importer.batch_import(
                [
                    {
                        "file_obj": file_bytes,
                        "filename": "robinhood.csv",
                        "account_id": account.id,
                    }
                ],
                config=config
            )

            assert result["success"] is True
            details = result["details"][0]
            assert details["imported"] == 2

            session = db_manager.get_session()
            try:
                rows = session.query(Transaction).order_by(Transaction.date.asc()).all()
                assert len(rows) == 2
                amounts = {row.description: row.amount for row in rows}
                assert amounts["Purchase"] == -150.0
                assert amounts["Payment Thank You"] == 75.0
            finally:
                session.close()
        finally:
            db_manager.close()
    
    def test_fix_robinhood_transactions_updates_existing_rows(self, test_database):
        """Verify fix script inverts purchases and payments when applied."""
        db_manager = DatabaseManager(test_database)
        db_manager.create_tables()
        account_manager = AccountManager(db_manager)
        account = account_manager.create_account("Robinhood Gold Card", AccountType.CREDIT)
        assert account is not None

        session = db_manager.get_session()
        try:
            session.add_all([
                Transaction(
                    date=datetime(2024, 1, 3),
                    description="Robinhood Purchase",
                    amount=120.0,
                    account=account.name,
                    account_id=account.id,
                    source_file="test.csv",
                    duplicate_hash="hash1",
                ),
                Transaction(
                    date=datetime(2024, 1, 4),
                    description="Payment Thank You",
                    amount=-60.0,
                    account=account.name,
                    account_id=account.id,
                    source_file="test.csv",
                    duplicate_hash="hash2",
                ),
            ])
            session.commit()
        finally:
            session.close()

        fix_robinhood_transactions(
            db_manager,
            "Robinhood Gold Card",
            fix_purchases=True,
            fix_payments=True,
            dry_run=False
        )

        session = db_manager.get_session()
        try:
            amounts = {
                row.description: row.amount
                for row in session.query(Transaction).all()
            }
        finally:
            session.close()
            db_manager.close()

        assert amounts["Robinhood Purchase"] == -120.0
        assert amounts["Payment Thank You"] == 60.0

