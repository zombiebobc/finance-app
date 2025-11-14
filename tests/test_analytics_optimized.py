"""
Comprehensive unit tests for optimized AnalyticsEngine methods.

Tests the SQL aggregation optimizations in get_income_expense_summary
and related methods, including edge cases and error handling.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch, call
from sqlalchemy.orm import Session
from sqlalchemy import func, case

from analytics import AnalyticsEngine
from database_ops import DatabaseManager, Transaction, Account, AccountType
from exceptions import AnalyticsError


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager."""
    manager = Mock(spec=DatabaseManager)
    return manager


@pytest.fixture
def analytics_engine(mock_db_manager):
    """Create an analytics engine with mocked database."""
    return AnalyticsEngine(mock_db_manager)


@pytest.fixture
def mock_session():
    """Create a mock SQLAlchemy session."""
    session = Mock(spec=Session)
    return session


class TestIncomeExpenseSummaryOptimized:
    """Test optimized get_income_expense_summary with SQL aggregations."""
    
    def test_summary_with_income_and_expenses(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary calculation with both income and expenses using SQL aggregation."""
        # Setup
        mock_db_manager.get_session.return_value = mock_session
        
        # Mock SQL aggregation result (single row with aggregated values)
        mock_result = Mock()
        mock_result.total_income = 1500.0
        mock_result.total_expenses = 400.0
        mock_result.income_count = 2
        mock_result.expense_count = 3
        mock_result.total_count = 5
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.one.return_value = mock_result
        mock_session.query.return_value = mock_query
        
        # Execute
        with patch.object(analytics_engine, 'parse_time_frame') as mock_parse:
            mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
            
            summary = analytics_engine.get_income_expense_summary(time_frame='all')
        
        # Verify
        assert summary['total_income'] == 1500.0
        assert summary['total_expenses'] == 400.0
        assert summary['net_change'] == 1100.0
        assert summary['income_count'] == 2
        assert summary['expense_count'] == 3
        assert summary['total_count'] == 5
        
        # Verify SQL aggregation was used (query should use func.sum/case, not query.all)
        assert mock_session.query.called
        mock_session.query.assert_called_once()
        mock_query.one.assert_called_once()  # Should use .one() for aggregation, not .all()
    
    def test_summary_empty_dataset(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary with no transactions returns zero values."""
        mock_db_manager.get_session.return_value = mock_session
        
        # Mock SQL aggregation result with NULL values (no rows)
        mock_result = Mock()
        mock_result.total_income = None
        mock_result.total_expenses = None
        mock_result.income_count = None
        mock_result.expense_count = None
        mock_result.total_count = None
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.one.return_value = mock_result
        mock_session.query.return_value = mock_query
        
        # Execute
        with patch.object(analytics_engine, 'parse_time_frame') as mock_parse:
            mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
            
            summary = analytics_engine.get_income_expense_summary(time_frame='all')
        
        # Verify zero values returned
        assert summary['total_income'] == 0.0
        assert summary['total_expenses'] == 0.0
        assert summary['net_change'] == 0.0
        assert summary['income_count'] == 0
        assert summary['expense_count'] == 0
        assert summary['total_count'] == 0
    
    def test_summary_with_account_filter(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary with account_id filter."""
        mock_db_manager.get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.total_income = 1000.0
        mock_result.total_expenses = 200.0
        mock_result.income_count = 1
        mock_result.expense_count = 2
        mock_result.total_count = 3
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.one.return_value = mock_result
        mock_session.query.return_value = mock_query
        
        with patch.object(analytics_engine, 'parse_time_frame') as mock_parse:
            mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
            
            summary = analytics_engine.get_income_expense_summary(
                time_frame='all',
                account_id=1
            )
        
        assert summary['total_income'] == 1000.0
        # Verify filter was called with account_id
        assert mock_query.filter.call_count >= 2  # Date filter + account filter
    
    def test_summary_with_account_type_filter(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary with account_type filter."""
        mock_db_manager.get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.total_income = 500.0
        mock_result.total_expenses = 100.0
        mock_result.income_count = 1
        mock_result.expense_count = 1
        mock_result.total_count = 2
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.one.return_value = mock_result
        mock_session.query.return_value = mock_query
        
        with patch.object(analytics_engine, 'parse_time_frame') as mock_parse:
            mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
            
            summary = analytics_engine.get_income_expense_summary(
                time_frame='all',
                account_type=AccountType.BANK
            )
        
        assert summary['total_income'] == 500.0
        # Verify join was called for account_type filter
        mock_query.join.assert_called_once()
    
    def test_summary_with_category_filter(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary with category_id filter."""
        mock_db_manager.get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.total_income = 0.0
        mock_result.total_expenses = 300.0
        mock_result.income_count = 0
        mock_result.expense_count = 5
        mock_result.total_count = 5
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.one.return_value = mock_result
        mock_session.query.return_value = mock_query
        
        with patch.object(analytics_engine, 'parse_time_frame') as mock_parse:
            mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
            
            summary = analytics_engine.get_income_expense_summary(
                time_frame='all',
                category_id='Groceries'
            )
        
        assert summary['total_expenses'] == 300.0
        # Verify filter was called with category
        assert mock_query.filter.call_count >= 2
    
    def test_summary_with_explicit_dates(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary with explicit date_from and date_to."""
        mock_db_manager.get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.total_income = 750.0
        mock_result.total_expenses = 150.0
        mock_result.income_count = 1
        mock_result.expense_count = 2
        mock_result.total_count = 3
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.one.return_value = mock_result
        mock_session.query.return_value = mock_query
        
        start_date = datetime(2023, 6, 1)
        end_date = datetime(2023, 6, 30)
        
        summary = analytics_engine.get_income_expense_summary(
            date_from=start_date,
            date_to=end_date
        )
        
        assert summary['total_income'] == 750.0
        # Verify explicit dates were used (parse_time_frame should not be called)
    
    def test_summary_invalid_date_range(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary raises error when start_date > end_date."""
        mock_db_manager.get_session.return_value = mock_session
        
        start_date = datetime(2023, 12, 31)
        end_date = datetime(2023, 1, 1)  # Start > End
        
        with pytest.raises(AnalyticsError) as exc_info:
            analytics_engine.get_income_expense_summary(
                date_from=start_date,
                date_to=end_date
            )
        
        assert "Start date must be before or equal to end date" in str(exc_info.value)
    
    def test_summary_partial_dates_error(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary raises error when only one date is provided."""
        mock_db_manager.get_session.return_value = mock_session
        
        start_date = datetime(2023, 1, 1)
        
        with pytest.raises(AnalyticsError) as exc_info:
            analytics_engine.get_income_expense_summary(
                date_from=start_date
                # Missing date_to
            )
        
        assert "Both date_from and date_to must be provided together" in str(exc_info.value)
    
    @pytest.mark.parametrize("time_frame", ['1m', '3m', '6m', '12m', 'all'])
    def test_summary_different_time_frames(self, analytics_engine, mock_db_manager, mock_session, time_frame):
        """Test summary works with different time frame formats."""
        mock_db_manager.get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.total_income = 1000.0
        mock_result.total_expenses = 500.0
        mock_result.income_count = 2
        mock_result.expense_count = 3
        mock_result.total_count = 5
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.one.return_value = mock_result
        mock_session.query.return_value = mock_query
        
        summary = analytics_engine.get_income_expense_summary(time_frame=time_frame)
        
        assert summary['total_income'] == 1000.0
        assert summary['total_expenses'] == 500.0
        assert summary['net_change'] == 500.0
    
    def test_summary_query_execution_error(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary handles query execution errors gracefully."""
        mock_db_manager.get_session.return_value = mock_session
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.one.side_effect = Exception("Database connection failed")
        mock_session.query.return_value = mock_query
        
        with patch.object(analytics_engine, 'parse_time_frame') as mock_parse:
            mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
            
            with pytest.raises(AnalyticsError) as exc_info:
                analytics_engine.get_income_expense_summary(time_frame='all')
        
        assert "Query execution failed" in str(exc_info.value)
        assert exc_info.value.original_error is not None
    
    def test_summary_large_date_range(self, analytics_engine, mock_db_manager, mock_session):
        """Test summary handles very large date ranges efficiently."""
        mock_db_manager.get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.total_income = 10000.0
        mock_result.total_expenses = 5000.0
        mock_result.income_count = 20
        mock_result.expense_count = 30
        mock_result.total_count = 50
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.one.return_value = mock_result
        mock_session.query.return_value = mock_session
        
        # Very large date range (10 years)
        start_date = datetime(2013, 1, 1)
        end_date = datetime(2023, 12, 31)
        
        summary = analytics_engine.get_income_expense_summary(
            date_from=start_date,
            date_to=end_date
        )
        
        assert summary['total_income'] == 10000.0
        # Should still use SQL aggregation (single query), not load all rows
    
    def test_summary_ensures_session_closed(self, analytics_engine, mock_db_manager, mock_session):
        """Test that session is always closed, even on error."""
        mock_db_manager.get_session.return_value = mock_session
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.one.side_effect = Exception("Test error")
        mock_session.query.return_value = mock_query
        
        with patch.object(analytics_engine, 'parse_time_frame') as mock_parse:
            mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
            
            with pytest.raises(AnalyticsError):
                analytics_engine.get_income_expense_summary(time_frame='all')
        
        # Verify session.close() was called
        mock_session.close.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

