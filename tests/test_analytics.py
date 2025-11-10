"""
Unit tests for analytics module.

Tests data aggregation functions, time frame parsing,
and report generation.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch

from analytics import AnalyticsEngine
from report_generator import ReportGenerator
from database_ops import DatabaseManager, Transaction, Account, AccountType


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
def report_generator():
    """Create a report generator instance."""
    return ReportGenerator()


class TestTimeFrameParsing:
    """Test time frame parsing functionality."""
    
    def test_parse_all_time(self, analytics_engine):
        """Test parsing 'all' time frame."""
        start, end = analytics_engine.parse_time_frame('all')
        assert start.year == 1900
        assert end.year == datetime.now().year
    
    def test_parse_relative_months(self, analytics_engine):
        """Test parsing relative month formats."""
        now = datetime.now()
        
        # Test 1 month
        start, end = analytics_engine.parse_time_frame('1m')
        assert (now - start).days >= 28
        assert (now - start).days <= 31
        
        # Test 3 months
        start, end = analytics_engine.parse_time_frame('3m')
        assert (now - start).days >= 85
        assert (now - start).days <= 95
        
        # Test 12 months
        start, end = analytics_engine.parse_time_frame('12m')
        assert (now - start).days >= 355
        assert (now - start).days <= 370
    
    def test_parse_custom_date_range(self, analytics_engine):
        """Test parsing custom date ranges."""
        start, end = analytics_engine.parse_time_frame('2023-01-01:2023-12-31')
        assert start == datetime(2023, 1, 1)
        assert end == datetime(2023, 12, 31)
    
    def test_parse_invalid_format(self, analytics_engine):
        """Test handling of invalid time frame formats."""
        with pytest.raises(ValueError):
            analytics_engine.parse_time_frame('invalid')
        
        with pytest.raises(ValueError):
            analytics_engine.parse_time_frame('2023-13-01:2023-12-31')


class TestIncomExpenseSummary:
    """Test income/expense summary functionality."""
    
    @patch.object(AnalyticsEngine, 'parse_time_frame')
    def test_summary_with_data(self, mock_parse, mock_db_manager, analytics_engine):
        """Test summary calculation with transaction data."""
        # Setup mocks
        mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
        
        mock_session = Mock()
        mock_db_manager.get_session.return_value = mock_session
        
        # Create mock transactions
        mock_transactions = [
            Mock(amount=1000.0),  # Income
            Mock(amount=500.0),   # Income
            Mock(amount=-200.0),  # Expense
            Mock(amount=-150.0),  # Expense
            Mock(amount=-50.0),   # Expense
        ]
        
        mock_query = Mock()
        mock_query.filter.return_value.all.return_value = mock_transactions
        mock_session.query.return_value = mock_query
        
        # Execute
        summary = analytics_engine.get_income_expense_summary(time_frame='all')
        
        # Verify
        assert summary['total_income'] == 1500.0
        assert summary['total_expenses'] == 400.0
        assert summary['net_change'] == 1100.0
        assert summary['income_count'] == 2
        assert summary['expense_count'] == 3
        assert summary['total_count'] == 5
    
    @patch.object(AnalyticsEngine, 'parse_time_frame')
    def test_summary_no_data(self, mock_parse, mock_db_manager, analytics_engine):
        """Test summary with no transactions."""
        mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
        
        mock_session = Mock()
        mock_db_manager.get_session.return_value = mock_session
        
        mock_query = Mock()
        mock_query.filter.return_value.all.return_value = []
        mock_session.query.return_value = mock_query
        
        summary = analytics_engine.get_income_expense_summary(time_frame='all')
        
        assert summary['total_income'] == 0.0
        assert summary['total_expenses'] == 0.0
        assert summary['net_change'] == 0.0
        assert summary['total_count'] == 0


class TestCategoryBreakdown:
    """Test category breakdown functionality."""
    
    @patch.object(AnalyticsEngine, 'parse_time_frame')
    def test_category_breakdown(self, mock_parse, mock_db_manager, analytics_engine):
        """Test category breakdown calculation."""
        mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
        
        mock_session = Mock()
        mock_db_manager.get_session.return_value = mock_session
        
        # Mock query results
        mock_results = [
            ('Groceries', -300.0, 5),
            ('Gas', -150.0, 3),
            ('Restaurants', -100.0, 2),
        ]
        
        mock_query = Mock()
        mock_query.filter.return_value.group_by.return_value.all.return_value = mock_results
        mock_session.query.return_value = mock_query
        
        # Execute
        df = analytics_engine.get_category_breakdown(time_frame='all', expense_only=True)
        
        # Verify
        assert len(df) == 3
        assert df.iloc[0]['category'] == 'Groceries'
        assert df.iloc[0]['total'] == 300.0  # Should be absolute value
        assert df.iloc[0]['count'] == 5
        assert abs(df.iloc[0]['percentage'] - 54.5) < 0.1  # 300/550 * 100
    
    @patch.object(AnalyticsEngine, 'parse_time_frame')
    def test_category_breakdown_empty(self, mock_parse, mock_db_manager, analytics_engine):
        """Test category breakdown with no data."""
        mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
        
        mock_session = Mock()
        mock_db_manager.get_session.return_value = mock_session
        
        mock_query = Mock()
        mock_query.filter.return_value.group_by.return_value.all.return_value = []
        mock_session.query.return_value = mock_query
        
        df = analytics_engine.get_category_breakdown(time_frame='all')
        
        assert df.empty
        assert list(df.columns) == ['category', 'total', 'count', 'percentage']


class TestReportGeneration:
    """Test report generation functionality."""
    
    def test_format_currency(self, report_generator):
        """Test currency formatting."""
        assert report_generator.format_currency(1000.0) == "$1,000.00"
        assert report_generator.format_currency(0.99) == "$0.99"
        assert report_generator.format_currency(-50.5) == "$-50.50"
    
    def test_format_percentage(self, report_generator):
        """Test percentage formatting."""
        assert report_generator.format_percentage(50.0) == "50.0%"
        assert report_generator.format_percentage(33.333) == "33.3%"
        assert report_generator.format_percentage(0.0) == "0.0%"
    
    def test_income_expense_report(self, report_generator):
        """Test income/expense report generation."""
        summary = {
            'total_income': 5000.0,
            'total_expenses': 3000.0,
            'net_change': 2000.0,
            'income_count': 10,
            'expense_count': 25,
            'total_count': 35
        }
        
        report = report_generator.generate_income_expense_report(summary, '6m')
        
        assert 'INCOME & EXPENSE SUMMARY' in report
        assert '$5,000.00' in report
        assert '$3,000.00' in report
        assert '$2,000.00' in report
        assert '10 transactions' in report
        assert '25 transactions' in report
    
    def test_category_report(self, report_generator):
        """Test category breakdown report generation."""
        df = pd.DataFrame({
            'category': ['Groceries', 'Gas', 'Restaurants'],
            'total': [300.0, 150.0, 100.0],
            'count': [5, 3, 2],
            'percentage': [54.5, 27.3, 18.2]
        })
        
        report = report_generator.generate_category_report(df, '6m')
        
        assert 'CATEGORY BREAKDOWN' in report
        assert 'Groceries' in report
        assert '$300.00' in report
        assert '54.5%' in report
    
    def test_category_report_empty(self, report_generator):
        """Test category report with empty data."""
        df = pd.DataFrame(columns=['category', 'total', 'count', 'percentage'])
        
        report = report_generator.generate_category_report(df, '6m')
        
        assert 'No spending data found' in report
    
    def test_export_to_csv(self, report_generator, tmp_path):
        """Test CSV export functionality."""
        df = pd.DataFrame({
            'category': ['Groceries', 'Gas'],
            'total': [300.0, 150.0],
            'count': [5, 3]
        })
        
        output_path = tmp_path / "test_export.csv"
        report_generator.export_to_csv(df, output_path, "test_report")
        
        assert output_path.exists()
        
        # Read back and verify
        df_loaded = pd.read_csv(output_path)
        assert len(df_loaded) == 2
        assert list(df_loaded.columns) == ['category', 'total', 'count']


class TestMonthlyTrends:
    """Test monthly trends functionality."""
    
    @patch.object(AnalyticsEngine, 'parse_time_frame')
    def test_monthly_trends(self, mock_parse, mock_db_manager, analytics_engine):
        """Test monthly trends calculation."""
        mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
        
        mock_session = Mock()
        mock_db_manager.get_session.return_value = mock_session
        
        # Mock query results (year, month, amount)
        mock_results = [
            (2023, 1, 1000.0),
            (2023, 1, -500.0),
            (2023, 2, 1200.0),
            (2023, 2, -600.0),
        ]
        
        mock_query = Mock()
        mock_query.filter.return_value.all.return_value = mock_results
        mock_session.query.return_value = mock_query
        
        # Execute
        df = analytics_engine.get_monthly_trends(time_frame='all')
        
        # Verify
        assert len(df) == 2
        assert df.iloc[0]['year'] == 2023
        assert df.iloc[0]['month'] == 1
        assert df.iloc[0]['income'] == 1000.0
        assert df.iloc[0]['expenses'] == 500.0
        assert df.iloc[0]['net'] == 500.0
        assert df.iloc[0]['period'] == '2023-01'


class TestAccountSummary:
    """Test account summary functionality."""
    
    @patch.object(AnalyticsEngine, 'parse_time_frame')
    def test_account_summary(self, mock_parse, mock_db_manager, analytics_engine):
        """Test account summary calculation."""
        mock_parse.return_value = (datetime(2023, 1, 1), datetime(2023, 12, 31))
        
        mock_session = Mock()
        mock_db_manager.get_session.return_value = mock_session
        
        # Mock query results
        mock_results = [
            ('Checking', AccountType.BANK, 2000.0),
            ('Checking', AccountType.BANK, -1000.0),
            ('Credit Card', AccountType.CREDIT, -500.0),
        ]
        
        mock_query = Mock()
        mock_query.join.return_value.filter.return_value.all.return_value = mock_results
        mock_session.query.return_value = mock_query
        
        # Execute
        df = analytics_engine.get_account_summary(time_frame='all')
        
        # Verify
        assert len(df) == 2
        assert 'Checking' in df['account_name'].values
        assert 'Credit Card' in df['account_name'].values


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

