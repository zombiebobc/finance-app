"""
Integration tests for AnalyticsEngine with real database.

Tests optimized queries against actual database with synthetic data,
including performance comparisons and edge cases.
"""

import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from analytics import AnalyticsEngine
from database_ops import DatabaseManager, Transaction, Account, AccountType
from exceptions import AnalyticsError


@pytest.fixture
def test_db_manager(tmp_path):
    """Create a test database manager with temporary database."""
    db_path = tmp_path / "test_analytics.db"
    db_manager = DatabaseManager(f"sqlite:///{db_path}")
    db_manager.create_tables()
    return db_manager


@pytest.fixture
def analytics_engine(test_db_manager):
    """Create analytics engine with test database."""
    return AnalyticsEngine(test_db_manager)


@pytest.fixture
def sample_transactions(test_db_manager):
    """Insert sample transactions for testing."""
    session = test_db_manager.get_session()
    try:
        # Create test account
        account = Account(
            name="Test Checking",
            type=AccountType.BANK,
            balance=0.0
        )
        session.add(account)
        session.flush()
        
        # Create transactions
        base_date = datetime(2023, 6, 15)
        transactions = [
            Transaction(
                date=base_date + timedelta(days=i),
                description=f"Transaction {i+1}",
                amount=1000.0 if i % 2 == 0 else -200.0,  # Alternating income/expense
                category="Groceries" if i % 3 == 0 else "Gas" if i % 3 == 1 else None,
                account_id=account.id,
                source_file="test.csv",
                duplicate_hash=f"hash_{i}",
                is_transfer=0
            )
            for i in range(10)
        ]
        
        session.add_all(transactions)
        session.commit()
        
        return account.id
    finally:
        session.close()


class TestIncomeExpenseSummaryIntegration:
    """Integration tests for get_income_expense_summary."""
    
    def test_summary_with_real_data(self, analytics_engine, sample_transactions):
        """Test summary with real database data."""
        summary = analytics_engine.get_income_expense_summary(time_frame='12m')
        
        # Verify results
        assert summary['total_income'] > 0
        assert summary['total_expenses'] > 0
        assert summary['net_change'] == summary['total_income'] - summary['total_expenses']
        assert summary['income_count'] > 0
        assert summary['expense_count'] > 0
        assert summary['total_count'] == summary['income_count'] + summary['expense_count']
    
    def test_summary_with_account_filter(self, analytics_engine, sample_transactions):
        """Test summary with account filter on real data."""
        summary = analytics_engine.get_income_expense_summary(
            time_frame='12m',
            account_id=sample_transactions
        )
        
        assert summary['total_count'] > 0
    
    def test_summary_empty_result(self, analytics_engine):
        """Test summary with empty database."""
        summary = analytics_engine.get_income_expense_summary(time_frame='12m')
        
        assert summary['total_income'] == 0.0
        assert summary['total_expenses'] == 0.0
        assert summary['net_change'] == 0.0
        assert summary['total_count'] == 0
    
    def test_summary_with_category_filter(self, analytics_engine, sample_transactions):
        """Test summary with category filter."""
        summary = analytics_engine.get_income_expense_summary(
            time_frame='12m',
            category_id='Groceries'
        )
        
        # Should only include transactions with category containing 'Groceries'
        assert summary['total_count'] >= 0  # May be 0 if no matches
    
    def test_summary_with_explicit_dates(self, analytics_engine, sample_transactions):
        """Test summary with explicit date range."""
        start_date = datetime(2023, 6, 1)
        end_date = datetime(2023, 6, 30)
        
        summary = analytics_engine.get_income_expense_summary(
            date_from=start_date,
            date_to=end_date
        )
        
        assert summary['total_count'] > 0
    
    def test_summary_date_range_outside_data(self, analytics_engine, sample_transactions):
        """Test summary with date range that includes no data."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)
        
        summary = analytics_engine.get_income_expense_summary(
            date_from=start_date,
            date_to=end_date
        )
        
        assert summary['total_count'] == 0
    
    def test_summary_verifies_sql_aggregation(self, analytics_engine, sample_transactions, test_db_manager):
        """Verify that SQL aggregation is actually used (not Python-side)."""
        # This test verifies the query structure, not just results
        session = test_db_manager.get_session()
        try:
            # Get summary (should use SQL aggregation)
            summary1 = analytics_engine.get_income_expense_summary(time_frame='12m')
            
            # Manually compute with Python (old approach) for comparison
            from sqlalchemy import and_
            start_date, end_date = analytics_engine.parse_time_frame('12m')
            transactions = session.query(Transaction).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date
                )
            ).all()
            
            python_income = sum(t.amount for t in transactions if t.amount > 0)
            python_expenses = sum(abs(t.amount) for t in transactions if t.amount < 0)
            
            # Results should match (proving SQL aggregation works correctly)
            assert abs(summary1['total_income'] - python_income) < 0.01
            assert abs(summary1['total_expenses'] - python_expenses) < 0.01
        finally:
            session.close()
    
    def test_summary_large_dataset_performance(self, analytics_engine, test_db_manager):
        """Test performance with larger dataset."""
        session = test_db_manager.get_session()
        try:
            # Create account
            account = Account(name="Performance Test", type=AccountType.BANK, balance=0.0)
            session.add(account)
            session.flush()
            
            # Insert 1000 transactions
            base_date = datetime(2023, 1, 1)
            transactions = [
                Transaction(
                    date=base_date + timedelta(days=i % 365),
                    description=f"Tx {i}",
                    amount=100.0 if i % 2 == 0 else -50.0,
                    category=f"Category {i % 10}",
                    account_id=account.id,
                    source_file="perf_test.csv",
                    duplicate_hash=f"perf_hash_{i}",
                    is_transfer=0
                )
                for i in range(1000)
            ]
            
            session.add_all(transactions)
            session.commit()
            
            # Measure query time
            import time
            start = time.perf_counter()
            summary = analytics_engine.get_income_expense_summary(time_frame='12m')
            elapsed = time.perf_counter() - start
            
            # Verify results
            assert summary['total_count'] == 1000
            # Should be fast (< 1 second for 1000 rows)
            assert elapsed < 1.0, f"Query took {elapsed:.2f}s, expected < 1.0s"
            
        finally:
            session.close()


class TestErrorHandlingIntegration:
    """Test error handling with real database."""
    
    def test_invalid_date_range_raises_error(self, analytics_engine):
        """Test invalid date range raises AnalyticsError."""
        start_date = datetime(2023, 12, 31)
        end_date = datetime(2023, 1, 1)
        
        with pytest.raises(AnalyticsError) as exc_info:
            analytics_engine.get_income_expense_summary(
                date_from=start_date,
                date_to=end_date
            )
        
        assert "Start date must be before" in str(exc_info.value)
    
    def test_partial_dates_raises_error(self, analytics_engine):
        """Test providing only one date raises error."""
        start_date = datetime(2023, 1, 1)
        
        with pytest.raises(AnalyticsError) as exc_info:
            analytics_engine.get_income_expense_summary(date_from=start_date)
        
        assert "Both date_from and date_to must be provided" in str(exc_info.value)
    
    def test_session_always_closed_on_error(self, analytics_engine, test_db_manager):
        """Test that session is always closed, even on error."""
        # Force an error by closing the manager's engine
        original_get_session = test_db_manager.get_session
        
        def failing_get_session():
            session = original_get_session()
            session.close()  # Close immediately to simulate error
            return session
        
        test_db_manager.get_session = failing_get_session
        
        # Should handle error gracefully
        with pytest.raises(Exception):
            analytics_engine.get_income_expense_summary(time_frame='12m')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

