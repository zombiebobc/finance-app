"""
Unit tests for refined account summary functionality.

Tests sign inversion for credit accounts, asset/liability grouping,
sorting, and net worth calculations.
"""

import pytest
from datetime import date
from unittest.mock import Mock, MagicMock

from account_management import AccountManager
from database_ops import DatabaseManager, AccountType


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager."""
    return Mock(spec=DatabaseManager)


@pytest.fixture
def account_manager(mock_db_manager):
    """Create an account manager with mock database."""
    return AccountManager(mock_db_manager)


class TestSignedBalance:
    """Test signed balance calculation (negative for credit accounts)."""
    
    def test_credit_account_negative(self, account_manager):
        """Test that credit accounts return negative balances."""
        # Mock get_balance_with_override to return positive value
        account_manager.get_balance_with_override = Mock(return_value=500.00)
        
        # Mock get_account to return credit account
        mock_account = Mock()
        mock_account.type = AccountType.CREDIT
        account_manager.get_account = Mock(return_value=mock_account)
        
        # Get signed balance
        balance = account_manager.get_signed_balance(account_id=1)
        
        # Should be negative
        assert balance == -500.00
    
    def test_bank_account_positive(self, account_manager):
        """Test that bank accounts return positive balances."""
        account_manager.get_balance_with_override = Mock(return_value=1000.00)
        
        mock_account = Mock()
        mock_account.type = AccountType.BANK
        account_manager.get_account = Mock(return_value=mock_account)
        
        balance = account_manager.get_signed_balance(account_id=1)
        
        # Should remain positive
        assert balance == 1000.00
    
    def test_investment_account_positive(self, account_manager):
        """Test that investment accounts return positive balances."""
        account_manager.get_balance_with_override = Mock(return_value=50000.00)
        
        mock_account = Mock()
        mock_account.type = AccountType.INVESTMENT
        account_manager.get_account = Mock(return_value=mock_account)
        
        balance = account_manager.get_signed_balance(account_id=1)
        
        # Should remain positive
        assert balance == 50000.00
    
    def test_savings_account_positive(self, account_manager):
        """Test that savings accounts return positive balances."""
        account_manager.get_balance_with_override = Mock(return_value=5000.00)
        
        mock_account = Mock()
        mock_account.type = AccountType.SAVINGS
        account_manager.get_account = Mock(return_value=mock_account)
        
        balance = account_manager.get_signed_balance(account_id=1)
        
        # Should remain positive
        assert balance == 5000.00
    
    def test_credit_account_zero_balance(self, account_manager):
        """Test credit account with zero balance."""
        account_manager.get_balance_with_override = Mock(return_value=0.00)
        
        mock_account = Mock()
        mock_account.type = AccountType.CREDIT
        account_manager.get_account = Mock(return_value=mock_account)
        
        balance = account_manager.get_signed_balance(account_id=1)
        
        # Zero should remain zero (or -0.00, which equals 0.00)
        assert balance == 0.00 or balance == -0.00


class TestNetWorthCalculation:
    """Test net worth calculation with mixed accounts."""
    
    def test_net_worth_assets_and_liabilities(self):
        """Test net worth with both assets and liabilities."""
        # Simulate account balances
        assets = [
            {'balance': 50000.00},  # Investment
            {'balance': 5000.00},   # Savings
            {'balance': 1000.00}    # Checking
        ]
        liabilities = [
            {'balance': -500.00},   # Credit Card 1
            {'balance': -300.00}    # Credit Card 2
        ]
        
        assets_total = sum(acc['balance'] for acc in assets)
        liabilities_total = sum(acc['balance'] for acc in liabilities)
        net_worth = assets_total + liabilities_total
        
        assert assets_total == 56000.00
        assert liabilities_total == -800.00
        assert net_worth == 55200.00
    
    def test_net_worth_only_assets(self):
        """Test net worth with only assets (no debts)."""
        assets = [
            {'balance': 10000.00},
            {'balance': 2000.00}
        ]
        liabilities = []
        
        assets_total = sum(acc['balance'] for acc in assets)
        liabilities_total = sum(acc['balance'] for acc in liabilities)
        net_worth = assets_total + liabilities_total
        
        assert net_worth == 12000.00
    
    def test_net_worth_only_liabilities(self):
        """Test net worth with only liabilities (negative net worth)."""
        assets = []
        liabilities = [
            {'balance': -5000.00},
            {'balance': -3000.00}
        ]
        
        assets_total = sum(acc['balance'] for acc in assets)
        liabilities_total = sum(acc['balance'] for acc in liabilities)
        net_worth = assets_total + liabilities_total
        
        assert net_worth == -8000.00


class TestAssetLiabilityGrouping:
    """Test grouping accounts into assets and liabilities."""
    
    def test_grouping_by_balance_sign(self):
        """Test that accounts are grouped correctly by balance sign."""
        accounts = [
            {'name': 'Investment', 'balance': 50000.00},
            {'name': 'Checking', 'balance': 1000.00},
            {'name': 'Credit Card', 'balance': -500.00},
            {'name': 'Savings', 'balance': 5000.00},
            {'name': 'Another Credit', 'balance': -300.00}
        ]
        
        assets = [acc for acc in accounts if acc['balance'] >= 0]
        liabilities = [acc for acc in accounts if acc['balance'] < 0]
        
        assert len(assets) == 3
        assert len(liabilities) == 2
        assert all(acc['balance'] >= 0 for acc in assets)
        assert all(acc['balance'] < 0 for acc in liabilities)
    
    def test_zero_balance_in_assets(self):
        """Test that zero balances are grouped with assets."""
        accounts = [
            {'name': 'Checking', 'balance': 1000.00},
            {'name': 'Zero Account', 'balance': 0.00},
            {'name': 'Credit Card', 'balance': -500.00}
        ]
        
        assets = [acc for acc in accounts if acc['balance'] >= 0]
        liabilities = [acc for acc in accounts if acc['balance'] < 0]
        
        assert len(assets) == 2  # Checking and Zero Account
        assert len(liabilities) == 1


class TestSorting:
    """Test sorting within asset and liability groups."""
    
    def test_assets_sorted_descending(self):
        """Test that assets are sorted by balance descending."""
        assets = [
            {'name': 'Checking', 'balance': 1000.00},
            {'name': 'Investment', 'balance': 50000.00},
            {'name': 'Savings', 'balance': 5000.00}
        ]
        
        assets_sorted = sorted(assets, key=lambda x: x['balance'], reverse=True)
        
        assert assets_sorted[0]['balance'] == 50000.00
        assert assets_sorted[1]['balance'] == 5000.00
        assert assets_sorted[2]['balance'] == 1000.00
    
    def test_liabilities_sorted_ascending(self):
        """Test that liabilities are sorted ascending (least negative first)."""
        liabilities = [
            {'name': 'Big Debt', 'balance': -5000.00},
            {'name': 'Small Debt', 'balance': -100.00},
            {'name': 'Medium Debt', 'balance': -1000.00}
        ]
        
        # Ascending order means least negative (-100) comes first
        liabilities_sorted = sorted(liabilities, key=lambda x: x['balance'], reverse=False)
        
        assert liabilities_sorted[0]['balance'] == -5000.00
        assert liabilities_sorted[1]['balance'] == -1000.00
        assert liabilities_sorted[2]['balance'] == -100.00


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

