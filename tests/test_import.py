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
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_ingestion import CSVReader
from data_standardization import DataStandardizer
from duplicate_detection import DuplicateDetector
from database_ops import DatabaseManager, Transaction, Base


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

