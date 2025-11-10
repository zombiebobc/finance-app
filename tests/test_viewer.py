"""
Unit tests for financial transaction viewer.

Tests cover:
- Data viewer query and filter functions
- CLI viewer argument parsing
- Filter validation
- Summary statistics
"""

import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime, date
import pandas as pd
from sqlalchemy import create_engine

from data_viewer import DataViewer
from database_ops import DatabaseManager, Transaction, Base
from duplicate_detection import DuplicateDetector


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
def sample_transactions(test_database):
    """Create sample transactions in the database."""
    detector = DuplicateDetector(["date", "description", "amount"])
    db_manager = DatabaseManager(test_database)
    
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
            "category": "Transportation",
            "source_file": "test.csv",
            "duplicate_hash": detector.generate_hash({
                "date": datetime(2024, 1, 16),
                "description": "Gas Station",
                "amount": -30.00
            })
        },
        {
            "date": datetime(2024, 1, 17),
            "description": "Salary Deposit",
            "amount": 2500.00,
            "category": "Income",
            "source_file": "test.csv",
            "duplicate_hash": detector.generate_hash({
                "date": datetime(2024, 1, 17),
                "description": "Salary Deposit",
                "amount": 2500.00
            })
        },
        {
            "date": datetime(2024, 2, 1),
            "description": "AMAZON Purchase",
            "amount": -99.99,
            "category": "Shopping",
            "source_file": "test2.csv",
            "duplicate_hash": detector.generate_hash({
                "date": datetime(2024, 2, 1),
                "description": "AMAZON Purchase",
                "amount": -99.99
            })
        }
    ]
    
    db_manager.insert_transactions(transactions)
    
    yield db_manager
    
    db_manager.close()


class TestDataViewer:
    """Tests for DataViewer class."""
    
    def test_get_transactions_df_no_filters(self, test_database, sample_transactions):
        """Test getting all transactions without filters."""
        viewer = DataViewer(sample_transactions)
        df = viewer.get_transactions_df()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4
        assert "date" in df.columns
        assert "description" in df.columns
        assert "amount" in df.columns
    
    def test_get_transactions_df_date_filter(self, test_database, sample_transactions):
        """Test filtering by date range."""
        viewer = DataViewer(sample_transactions)
        
        # Filter by date range
        filters = {
            "date_start": "2024-01-16",
            "date_end": "2024-01-17"
        }
        df = viewer.get_transactions_df(filters=filters)
        
        assert len(df) == 2
        assert all(df["date"].dt.date >= date(2024, 1, 16))
        assert all(df["date"].dt.date <= date(2024, 1, 17))
    
    def test_get_transactions_df_amount_filter(self, test_database, sample_transactions):
        """Test filtering by amount range."""
        viewer = DataViewer(sample_transactions)
        
        # Filter negative amounts only
        filters = {
            "amount_max": 0
        }
        df = viewer.get_transactions_df(filters=filters)
        
        assert len(df) == 3
        assert all(df["amount"] <= 0)
    
    def test_get_transactions_df_description_filter(self, test_database, sample_transactions):
        """Test filtering by description keywords."""
        viewer = DataViewer(sample_transactions)
        
        filters = {
            "description_keywords": "AMAZON"
        }
        df = viewer.get_transactions_df(filters=filters)
        
        assert len(df) == 1
        assert "AMAZON" in df.iloc[0]["description"].upper()
    
    def test_get_transactions_df_category_filter(self, test_database, sample_transactions):
        """Test filtering by category."""
        viewer = DataViewer(sample_transactions)
        
        filters = {
            "category": "Shopping"
        }
        df = viewer.get_transactions_df(filters=filters)
        
        assert len(df) == 1
        assert "Shopping" in df.iloc[0]["category"]
    
    def test_get_transactions_df_limit(self, test_database, sample_transactions):
        """Test limiting results."""
        viewer = DataViewer(sample_transactions)
        df = viewer.get_transactions_df(limit=2)
        
        assert len(df) == 2
    
    def test_get_transactions_df_offset(self, test_database, sample_transactions):
        """Test pagination with offset."""
        viewer = DataViewer(sample_transactions)
        df1 = viewer.get_transactions_df(limit=2, offset=0)
        df2 = viewer.get_transactions_df(limit=2, offset=2)
        
        assert len(df1) == 2
        assert len(df2) == 2
        # Should have different transactions
        assert df1.iloc[0]["id"] != df2.iloc[0]["id"]
    
    def test_get_transactions_df_sorting(self, test_database, sample_transactions):
        """Test sorting."""
        viewer = DataViewer(sample_transactions)
        
        # Sort by amount ascending
        df_asc = viewer.get_transactions_df(order_by="amount", order_desc=False)
        assert df_asc.iloc[0]["amount"] < df_asc.iloc[-1]["amount"]
        
        # Sort by amount descending
        df_desc = viewer.get_transactions_df(order_by="amount", order_desc=True)
        assert df_desc.iloc[0]["amount"] > df_desc.iloc[-1]["amount"]
    
    def test_format_transactions_df(self, test_database, sample_transactions):
        """Test formatting transactions DataFrame."""
        viewer = DataViewer(sample_transactions)
        df = viewer.get_transactions_df()
        formatted_df = viewer.format_transactions_df(df)
        
        # Check date formatting
        assert formatted_df["date"].dtype == "object"  # Should be string
        assert formatted_df.iloc[0]["date"] == "2024-01-15" or formatted_df.iloc[0]["date"] == "2024-02-01"
        
        # Check amount formatting column exists
        assert "amount_formatted" in formatted_df.columns
    
    def test_get_summary_stats(self, test_database, sample_transactions):
        """Test getting summary statistics."""
        viewer = DataViewer(sample_transactions)
        stats = viewer.get_summary_stats()
        
        assert stats["total_count"] == 4
        assert stats["total_amount"] == pytest.approx(2324.51)  # -45.50 - 30.00 + 2500.00 - 99.99
        assert stats["positive_count"] == 1
        assert stats["negative_count"] == 3
        assert stats["positive_total"] == pytest.approx(2500.00)
        assert stats["negative_total"] == pytest.approx(-175.49)
    
    def test_get_summary_stats_with_filters(self, test_database, sample_transactions):
        """Test summary statistics with filters."""
        viewer = DataViewer(sample_transactions)
        
        filters = {"amount_max": 0}  # Only negative amounts
        stats = viewer.get_summary_stats(filters=filters)
        
        assert stats["total_count"] == 3
        assert stats["positive_count"] == 0
        assert stats["negative_count"] == 3
    
    def test_validate_filters_invalid_date(self, test_database, sample_transactions):
        """Test filter validation with invalid date."""
        viewer = DataViewer(sample_transactions)
        
        filters = {"date_start": "invalid-date"}
        with pytest.raises(ValueError):
            viewer.get_transactions_df(filters=filters)
    
    def test_validate_filters_invalid_amount(self, test_database, sample_transactions):
        """Test filter validation with invalid amount."""
        viewer = DataViewer(sample_transactions)
        
        filters = {"amount_min": "not-a-number"}
        with pytest.raises(ValueError):
            viewer.get_transactions_df(filters=filters)
    
    def test_validate_filters_date_range(self, test_database, sample_transactions):
        """Test filter validation with invalid date range."""
        viewer = DataViewer(sample_transactions)
        
        filters = {
            "date_start": "2024-01-20",
            "date_end": "2024-01-10"  # End before start
        }
        with pytest.raises(ValueError):
            viewer.get_transactions_df(filters=filters)
    
    def test_validate_filters_amount_range(self, test_database, sample_transactions):
        """Test filter validation with invalid amount range."""
        viewer = DataViewer(sample_transactions)
        
        filters = {
            "amount_min": 100,
            "amount_max": 50  # Max less than min
        }
        with pytest.raises(ValueError):
            viewer.get_transactions_df(filters=filters)
    
    def test_export_to_csv(self, test_database, sample_transactions):
        """Test exporting to CSV."""
        viewer = DataViewer(sample_transactions)
        df = viewer.get_transactions_df()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            viewer.export_to_csv(df, temp_path)
            
            # Verify file exists and has content
            assert os.path.exists(temp_path)
            assert os.path.getsize(temp_path) > 0)
            
            # Verify can read it back
            df_read = pd.read_csv(temp_path)
            assert len(df_read) == len(df)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_empty_database(self, test_database):
        """Test handling empty database."""
        db_manager = DatabaseManager(test_database)
        viewer = DataViewer(db_manager)
        
        df = viewer.get_transactions_df()
        assert df.empty
        assert len(df) == 0
        
        stats = viewer.get_summary_stats()
        assert stats["total_count"] == 0
        assert stats["total_amount"] == 0.0
        
        db_manager.close()


class TestCLIViewer:
    """Tests for CLI viewer argument parsing."""
    
    def test_build_filters(self):
        """Test building filters from arguments."""
        from cli_viewer import build_filters
        from argparse import Namespace
        
        args = Namespace(
            date_start="2024-01-01",
            date_end="2024-12-31",
            amount_min=-100.0,
            amount_max=1000.0,
            description="AMAZON",
            category="Shopping",
            source_file=None
        )
        
        filters = build_filters(args)
        
        assert filters["date_start"] == "2024-01-01"
        assert filters["date_end"] == "2024-12-31"
        assert filters["amount_min"] == -100.0
        assert filters["amount_max"] == 1000.0
        assert filters["description_keywords"] == "AMAZON"
        assert filters["category"] == "Shopping"
        assert "source_file" not in filters

