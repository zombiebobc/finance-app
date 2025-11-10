"""
Tests for enhanced overview page functionality.

This module tests the data fetching, balance calculations,
and configuration management for the improved account section.
"""

import pytest
from datetime import date, timedelta
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from data_fetch import (
    fetch_account_summaries,
    calculate_historical_balance,
    fetch_balance_history,
    fetch_net_worth_history,
    get_time_frame_dates
)
from viz_components import (
    format_currency,
    ACCOUNT_ICONS,
    COLORS
)
from config_manager import (
    load_config,
    save_config,
    get_net_worth_goal,
    set_net_worth_goal,
    DEFAULT_CONFIG
)


class TestDataFetch:
    """Tests for data fetching functions."""
    
    def test_fetch_account_summaries_empty(self):
        """Test fetching account summaries with no accounts."""
        db_manager = Mock()
        account_manager_mock = Mock()
        account_manager_mock.list_accounts.return_value = []
        
        with patch('data_fetch.AccountManager', return_value=account_manager_mock):
            result = fetch_account_summaries(db_manager)
        
        assert result['assets'].empty
        assert result['liabilities'].empty
        assert result['net_worth'] == 0.0
        assert result['assets_total'] == 0.0
        assert result['liabilities_total'] == 0.0
    
    def test_fetch_account_summaries_with_data(self):
        """Test fetching account summaries with accounts."""
        db_manager = Mock()
        
        # Mock accounts
        account1 = Mock()
        account1.id = 1
        account1.name = "Checking"
        account1.type.value = "bank"
        
        account2 = Mock()
        account2.id = 2
        account2.name = "Credit Card"
        account2.type.value = "credit"
        
        account_manager_mock = Mock()
        account_manager_mock.list_accounts.return_value = [account1, account2]
        account_manager_mock.get_signed_balance.side_effect = [1000.0, -500.0]
        
        with patch('data_fetch.AccountManager', return_value=account_manager_mock):
            result = fetch_account_summaries(db_manager, date.today())
        
        assert len(result['assets']) == 1
        assert len(result['liabilities']) == 1
        assert result['net_worth'] == 500.0
        assert result['assets_total'] == 1000.0
        assert result['liabilities_total'] == -500.0
    
    def test_calculate_historical_balance(self):
        """Test calculating historical balance."""
        db_manager = Mock()
        account_manager_mock = Mock()
        account_manager_mock.get_signed_balance.return_value = 1500.0
        
        with patch('data_fetch.AccountManager', return_value=account_manager_mock):
            balance = calculate_historical_balance(db_manager, 1, date(2024, 1, 1))
        
        assert balance == 1500.0
        account_manager_mock.get_signed_balance.assert_called_once_with(1, date(2024, 1, 1))
    
    def test_fetch_balance_history_no_transactions(self):
        """Test fetching balance history with no transactions."""
        db_manager = Mock()
        session_mock = Mock()
        session_mock.query.return_value.filter.return_value.count.return_value = 0
        db_manager.get_session.return_value = session_mock
        
        account_manager_mock = Mock()
        account_manager_mock.get_account.return_value = Mock(id=1, name="Test")
        account_manager_mock.get_signed_balance.return_value = 1000.0
        
        with patch('data_fetch.AccountManager', return_value=account_manager_mock):
            df = fetch_balance_history(db_manager, 1, days=7)
        
        assert len(df) == 8  # 7 days + today
        assert 'date' in df.columns
        assert 'balance' in df.columns
        assert all(df['balance'] == 1000.0)
    
    def test_get_time_frame_dates_current(self):
        """Test getting time frame dates for 'Current'."""
        start, end = get_time_frame_dates('Current')
        today = date.today()
        assert start == today
        assert end == today
    
    def test_get_time_frame_dates_last_month(self):
        """Test getting time frame dates for 'Last Month'."""
        start, end = get_time_frame_dates('Last Month')
        today = date.today()
        first_of_month = today.replace(day=1)
        expected_end = first_of_month - timedelta(days=1)
        expected_start = expected_end.replace(day=1)
        assert start == expected_start
        assert end == expected_end
    
    def test_get_time_frame_dates_custom(self):
        """Test getting time frame dates for custom date."""
        custom_date = '2024-01-15'
        start, end = get_time_frame_dates(custom_date)
        assert start == date(2024, 1, 15)
        assert end == date(2024, 1, 15)


class TestVizComponents:
    """Tests for visualization components."""
    
    def test_format_currency_positive(self):
        """Test formatting positive currency."""
        assert format_currency(1234.56) == "$1,234.56"
    
    def test_format_currency_negative(self):
        """Test formatting negative currency."""
        assert format_currency(-1234.56) == "$-1,234.56"
    
    def test_format_currency_zero(self):
        """Test formatting zero currency."""
        assert format_currency(0.0) == "$0.00"
    
    def test_account_icons_exist(self):
        """Test that all expected account type icons exist."""
        assert 'bank' in ACCOUNT_ICONS
        assert 'credit' in ACCOUNT_ICONS
        assert 'investment' in ACCOUNT_ICONS
        assert 'savings' in ACCOUNT_ICONS
        assert 'cash' in ACCOUNT_ICONS
        assert 'other' in ACCOUNT_ICONS
    
    def test_colors_defined(self):
        """Test that color scheme is defined."""
        assert 'positive' in COLORS
        assert 'negative' in COLORS
        assert 'neutral' in COLORS
        assert 'primary' in COLORS


class TestConfigManager:
    """Tests for configuration management."""
    
    def test_load_config_defaults(self):
        """Test loading config with defaults."""
        with patch('config_manager.Path') as path_mock:
            path_mock.return_value.exists.return_value = False
            config = load_config()
        
        assert config['net_worth_goal'] == DEFAULT_CONFIG['net_worth_goal']
        assert config['show_sparklines'] == DEFAULT_CONFIG['show_sparklines']
    
    def test_save_config(self):
        """Test saving configuration."""
        test_config = {'net_worth_goal': 150000.0}
        
        with patch('config_manager.Path') as path_mock, \
             patch('builtins.open', create=True) as open_mock, \
             patch('config_manager.yaml.dump') as yaml_dump_mock, \
             patch('config_manager.yaml.safe_load', return_value={}):
            
            path_mock.return_value.exists.return_value = True
            result = save_config(test_config)
        
        assert result is True
    
    @patch('config_manager.st')
    @patch('config_manager.load_config')
    def test_get_net_worth_goal_from_session(self, load_config_mock, st_mock):
        """Test getting net worth goal from session state."""
        # Create a mock object that supports attribute access
        session_state_mock = Mock()
        session_state_mock.__contains__ = lambda self, key: key == 'net_worth_goal'
        session_state_mock.net_worth_goal = 200000.0
        st_mock.session_state = session_state_mock
        
        goal = get_net_worth_goal()
        assert goal == 200000.0
    
    @patch('config_manager.st')
    def test_set_net_worth_goal(self, st_mock):
        """Test setting net worth goal."""
        # Create a mock object that supports attribute assignment
        session_state_mock = Mock()
        st_mock.session_state = session_state_mock
        
        result = set_net_worth_goal(250000.0, save_to_file=False)
        assert result is True
        assert session_state_mock.net_worth_goal == 250000.0


class TestIntegration:
    """Integration tests for the overview page components."""
    
    def test_fetch_and_display_workflow(self):
        """Test the full workflow of fetching and displaying account data."""
        # Mock database and account manager
        db_manager = Mock()
        
        account1 = Mock()
        account1.id = 1
        account1.name = "Savings"
        account1.type.value = "savings"
        
        account_manager_mock = Mock()
        account_manager_mock.list_accounts.return_value = [account1]
        account_manager_mock.get_signed_balance.return_value = 5000.0
        
        with patch('data_fetch.AccountManager', return_value=account_manager_mock):
            summary = fetch_account_summaries(db_manager)
        
        # Verify data structure
        assert 'assets' in summary
        assert 'liabilities' in summary
        assert 'net_worth' in summary
        assert summary['net_worth'] == 5000.0
        
        # Verify assets DataFrame has expected columns
        assert list(summary['assets'].columns) == ['id', 'name', 'type', 'balance']
        assert len(summary['assets']) == 1
    
    def test_balance_history_calculation(self):
        """Test balance history calculation workflow."""
        db_manager = Mock()
        session_mock = Mock()
        db_manager.get_session.return_value = session_mock
        
        # Mock query to return transactions
        session_mock.query.return_value.filter.return_value.count.return_value = 5
        
        account_manager_mock = Mock()
        account_manager_mock.get_account.return_value = Mock(id=1, name="Test")
        
        # Mock balance changes over time
        balances = [1000.0, 1100.0, 1200.0, 1300.0, 1400.0, 1500.0, 1600.0, 1700.0]
        account_manager_mock.get_signed_balance.side_effect = balances
        
        with patch('data_fetch.AccountManager', return_value=account_manager_mock):
            df = fetch_balance_history(db_manager, 1, days=7)
        
        # Verify we get daily data
        assert len(df) == 8
        assert 'date' in df.columns
        assert 'balance' in df.columns
        assert df['balance'].tolist() == balances


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

