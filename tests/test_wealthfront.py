"""
Unit tests for Wealthfront import functionality.

Tests transfer detection, balance updates, and balance history tracking.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

from manual_update import detect_wealthfront_transfers, prompt_balance_update_cli
from account_management import AccountManager
from database_ops import DatabaseManager, AccountType


@pytest.fixture
def mock_config():
    """Create a mock configuration."""
    return {
        'wealthfront': {
            'transfer_patterns': [
                r'[Tt]ransfer\s+to\s+[Aa]utomated\s+[Ii]nvesting',
                r'[Tt]ransfer\s+to\s+[Ii]nvestment'
            ],
            'cash_account_name': 'Wealthfront Cash Savings',
            'investment_account_name': 'Wealthfront Automated Investment'
        }
    }


class TestWealthfrontTransferDetection:
    """Test transfer detection functionality."""
    
    def test_detect_transfers_basic(self, mock_config):
        """Test basic transfer detection."""
        transactions = [
            {'description': 'Transfer to Automated Investing', 'amount': -500.00},
            {'description': 'Coffee Shop Purchase', 'amount': -5.50},
            {'description': 'Transfer to Investment', 'amount': -100.00}
        ]
        
        transfers = detect_wealthfront_transfers(transactions, mock_config)
        
        assert len(transfers) == 2
        assert transfers[0]['description'] == 'Transfer to Automated Investing'
        assert transfers[1]['description'] == 'Transfer to Investment'
    
    def test_detect_transfers_case_insensitive(self, mock_config):
        """Test case-insensitive detection."""
        transactions = [
            {'description': 'TRANSFER TO AUTOMATED INVESTING', 'amount': -200.00},
            {'description': 'transfer to investment', 'amount': -150.00}
        ]
        
        transfers = detect_wealthfront_transfers(transactions, mock_config)
        
        assert len(transfers) == 2
    
    def test_detect_transfers_no_match(self, mock_config):
        """Test when no transfers are found."""
        transactions = [
            {'description': 'Restaurant', 'amount': -25.00},
            {'description': 'Paycheck', 'amount': 2000.00}
        ]
        
        transfers = detect_wealthfront_transfers(transactions, mock_config)
        
        assert len(transfers) == 0
    
    def test_detect_transfers_empty_list(self, mock_config):
        """Test with empty transaction list."""
        transfers = detect_wealthfront_transfers([], mock_config)
        
        assert len(transfers) == 0


class TestBalanceUpdate:
    """Test balance update functionality."""
    
    @patch('account_management.Account')
    def test_update_balance(self, mock_account):
        """Test updating account balance."""
        mock_db_manager = Mock(spec=DatabaseManager)
        account_manager = AccountManager(mock_db_manager)
        
        # Mock session
        mock_session = Mock()
        mock_db_manager.get_session.return_value = mock_session
        
        # Mock account
        mock_acc = Mock()
        mock_acc.id = 1
        mock_acc.name = "Investment Account"
        mock_acc.balance = 1000.0
        mock_session.query.return_value.filter.return_value.first.return_value = mock_acc
        
        # Update balance
        success = account_manager.update_balance(
            account_id=1,
            new_balance=1500.0,
            notes="Test update"
        )
        
        assert success
        assert mock_acc.balance == 1500.0
        mock_session.commit.assert_called_once()
    
    def test_update_balance_negative(self):
        """Test that negative balances are allowed (for losses)."""
        mock_db_manager = Mock(spec=DatabaseManager)
        account_manager = AccountManager(mock_db_manager)
        
        mock_session = Mock()
        mock_db_manager.get_session.return_value = mock_session
        
        mock_acc = Mock()
        mock_acc.id = 1
        mock_acc.name = "Investment Account"
        mock_acc.balance = 1000.0
        mock_session.query.return_value.filter.return_value.first.return_value = mock_acc
        
        success = account_manager.update_balance(
            account_id=1,
            new_balance=-500.0,
            notes="Portfolio loss"
        )
        
        assert success
        assert mock_acc.balance == -500.0


class TestAccountCreation:
    """Test account creation and retrieval."""
    
    def test_get_or_create_existing(self):
        """Test getting existing account."""
        mock_db_manager = Mock(spec=DatabaseManager)
        account_manager = AccountManager(mock_db_manager)
        
        # Mock existing account
        account_manager.get_account_by_name = Mock(return_value={'id': 1, 'name': 'Test Account'})
        
        result = account_manager.get_or_create_account(
            'Test Account',
            AccountType.SAVINGS,
            1000.0
        )
        
        assert result == {'id': 1, 'name': 'Test Account'}
        account_manager.get_account_by_name.assert_called_once_with('Test Account')
    
    def test_get_or_create_new(self):
        """Test creating new account when it doesn't exist."""
        mock_db_manager = Mock(spec=DatabaseManager)
        account_manager = AccountManager(mock_db_manager)
        
        # Mock no existing account
        account_manager.get_account_by_name = Mock(return_value=None)
        mock_new_account = {'id': 2, 'name': 'New Account'}
        account_manager.create_account = Mock(return_value=mock_new_account)
        
        result = account_manager.get_or_create_account(
            'New Account',
            AccountType.SAVINGS,
            500.0
        )
        
        assert result == mock_new_account
        account_manager.create_account.assert_called_once_with('New Account', AccountType.SAVINGS, 500.0)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

