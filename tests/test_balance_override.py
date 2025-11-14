"""
Unit tests for balance override functionality.

Tests setting overrides, calculating balances with overrides,
and handling edge cases like multiple overrides and historical dates.
"""

import pytest
from datetime import date, datetime, timedelta
from unittest.mock import Mock, MagicMock

from account_management import AccountManager
from database_ops import DatabaseManager, AccountType
from uuid import uuid4
from datetime import datetime


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager."""
    return Mock(spec=DatabaseManager)


@pytest.fixture
def account_manager(mock_db_manager):
    """Create an account manager with mock database."""
    return AccountManager(mock_db_manager)


@pytest.fixture
def account_manager_db(tmp_path):
    """Create an account manager backed by a real temporary database."""
    db_path = tmp_path / "balance_override.db"
    manager = DatabaseManager(f"sqlite:///{db_path}")
    manager.create_tables()
    account_mgr = AccountManager(manager)
    yield account_mgr
    manager.close()
    if db_path.exists():
        db_path.unlink()


class TestBalanceOverride:
    """Test balance override functionality."""
    
    def test_set_balance_override_basic(self, account_manager):
        """Test setting a basic balance override."""
        mock_session = Mock()
        account_manager.db_manager.get_session.return_value = mock_session
        
        # Mock account
        mock_account = Mock()
        mock_account.id = 1
        mock_account.name = "Test Account"
        mock_session.query.return_value.filter.return_value.first.return_value = mock_account
        
        # Set override
        success = account_manager.set_balance_override(
            account_id=1,
            override_date=date(2024, 1, 1),
            override_balance=5000.00,
            notes="Opening balance"
        )
        
        assert success
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
    
    def test_set_balance_override_future_date(self, account_manager):
        """Test setting override with future date (should warn but allow)."""
        mock_session = Mock()
        account_manager.db_manager.get_session.return_value = mock_session
        
        # Mock account
        mock_account = Mock()
        mock_account.id = 1
        mock_account.name = "Test Account"
        mock_session.query.return_value.filter.return_value.first.return_value = mock_account
        
        # Set override with future date
        future_date = date.today() + timedelta(days=30)
        success = account_manager.set_balance_override(
            account_id=1,
            override_date=future_date,
            override_balance=1000.00
        )
        
        # Should still succeed (with warning logged)
        assert success
    
    def test_set_balance_override_nonexistent_account(self, account_manager):
        """Test setting override for nonexistent account."""
        mock_session = Mock()
        account_manager.db_manager.get_session.return_value = mock_session
        
        # No account found
        mock_session.query.return_value.filter.return_value.first.return_value = None
        
        # Set override
        success = account_manager.set_balance_override(
            account_id=999,
            override_date=date(2024, 1, 1),
            override_balance=5000.00
        )
        
        assert not success


class TestBalanceCalculationWithOverride:
    """Test balance calculations with overrides."""
    
    def test_balance_with_override_no_transactions(self, account_manager_db):
        """Test balance calculation with override but no transactions after."""
        account = account_manager_db.create_account("Test Account", AccountType.BANK)
        override_date = date(2024, 1, 1)
        account_manager_db.set_balance_override(account.id, override_date, 5000.00)
        
        balance = account_manager_db.get_balance_with_override(account.id, date.today())
        assert balance == 5000.00
    
    def test_balance_with_override_and_transactions(self, account_manager_db):
        """Test balance calculation with override and subsequent transactions."""
        account = account_manager_db.create_account("Test Account", AccountType.BANK)
        override_date = date(2024, 1, 1)
        account_manager_db.set_balance_override(account.id, override_date, 5000.00)
        
        db_manager = account_manager_db.db_manager
        db_manager.insert_transactions([{
            "date": datetime(2024, 2, 1),
            "description": "Deposit",
            "amount": 1500.00,
            "category": "Income",
            "account_id": account.id,
            "account": account.name,
            "source_file": "test.csv",
            "duplicate_hash": f"hash-{uuid4()}",
            "is_transfer": 0
        }])
        
        balance = account_manager_db.get_balance_with_override(account.id, date.today())
        assert balance == 6500.00  # 5000 + 1500
    
    def test_balance_without_override(self, account_manager_db):
        """Test balance calculation without any override (full sum)."""
        account = account_manager_db.create_account("Test Account", AccountType.BANK)
        db_manager = account_manager_db.db_manager
        db_manager.insert_transactions([{
            "date": datetime(2024, 3, 1),
            "description": "Salary",
            "amount": 3000.00,
            "category": "Income",
            "account_id": account.id,
            "account": account.name,
            "source_file": "test.csv",
            "duplicate_hash": f"hash-{uuid4()}",
            "is_transfer": 0
        }])
        
        balance = account_manager_db.get_balance_with_override(account.id, date.today())
        assert balance == 3000.00
    
    def test_balance_as_of_past_date(self, account_manager_db):
        """Test balance calculation as of a past date."""
        account = account_manager_db.create_account("Test Account", AccountType.BANK)
        override_date = date(2024, 1, 1)
        account_manager_db.set_balance_override(account.id, override_date, 5000.00)
        
        db_manager = account_manager_db.db_manager
        db_manager.insert_transactions([{
            "date": datetime(2024, 4, 1),
            "description": "Bonus",
            "amount": 500.00,
            "category": "Income",
            "account_id": account.id,
            "account": account.name,
            "source_file": "test.csv",
            "duplicate_hash": f"hash-{uuid4()}",
            "is_transfer": 0
        }])
        
        past_date = date(2024, 6, 1)
        balance = account_manager_db.get_balance_with_override(account.id, past_date)
        assert balance == 5500.00


class TestBalanceOverrideManagement:
    """Test balance override management functions."""
    
    def test_get_balance_overrides(self, account_manager):
        """Test retrieving balance overrides for an account."""
        mock_session = Mock()
        account_manager.db_manager.get_session.return_value = mock_session
        
        # Mock overrides
        mock_override1 = Mock()
        mock_override1.id = 1
        mock_override1.override_date = date(2024, 1, 1)
        mock_override1.override_balance = 5000.00
        mock_override1.created_at = datetime(2024, 1, 1, 10, 0)
        mock_override1.notes = "First override"
        
        mock_override2 = Mock()
        mock_override2.id = 2
        mock_override2.override_date = date(2024, 6, 1)
        mock_override2.override_balance = 7000.00
        mock_override2.created_at = datetime(2024, 6, 1, 10, 0)
        mock_override2.notes = "Second override"
        
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
            mock_override1, mock_override2
        ]
        
        # Get overrides
        overrides = account_manager.get_balance_overrides(account_id=1)
        
        assert len(overrides) == 2
        assert overrides[0]['override_balance'] == 5000.00
        assert overrides[1]['override_balance'] == 7000.00
    
    def test_delete_balance_override(self, account_manager):
        """Test deleting a balance override."""
        mock_session = Mock()
        account_manager.db_manager.get_session.return_value = mock_session
        
        # Mock override
        mock_override = Mock()
        mock_override.id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_override
        
        # Delete override
        success = account_manager.delete_balance_override(override_id=1)
        
        assert success
        mock_session.delete.assert_called_once_with(mock_override)
        mock_session.commit.assert_called_once()
    
    def test_delete_nonexistent_override(self, account_manager):
        """Test deleting a nonexistent override."""
        mock_session = Mock()
        account_manager.db_manager.get_session.return_value = mock_session
        
        # No override found
        mock_session.query.return_value.filter.return_value.first.return_value = None
        
        # Delete override
        success = account_manager.delete_balance_override(override_id=999)
        
        assert not success


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

