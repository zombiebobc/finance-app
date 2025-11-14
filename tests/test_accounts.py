"""
Unit tests for account management and enhanced import features.

Tests cover:
- Account CRUD operations
- Account balance calculations
- Enhanced import with account linking
- Transfer detection
- Categorization rules
"""

import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime, date
import pandas as pd
from sqlalchemy import create_engine

from account_management import AccountManager
from database_ops import DatabaseManager, Account, AccountType, Transaction, Base
from enhanced_import import EnhancedImporter
from categorization import CategorizationEngine, CategorizationRule
from budgeting import BudgetManager


@pytest.fixture
def test_database():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        temp_db = f.name
    
    connection_string = f"sqlite:///{temp_db}"
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)
    
    try:
        yield connection_string
    finally:
        engine.dispose()
        if os.path.exists(temp_db):
            try:
                os.unlink(temp_db)
            except PermissionError:
                pass


class TestAccountManagement:
    """Tests for AccountManager class."""
    
    def test_create_account(self, test_database):
        """Test creating an account."""
        db_manager = DatabaseManager(test_database)
        account_manager = AccountManager(db_manager)
        
        account = account_manager.create_account(
            name="Test Checking",
            account_type=AccountType.BANK,
            initial_balance=1000.0
        )
        
        assert account is not None
        assert account.name == "Test Checking"
        assert account.type == AccountType.BANK
        assert account.balance == 1000.0
        
        db_manager.close()
    
    def test_create_duplicate_account(self, test_database):
        """Test that creating duplicate account names fails."""
        db_manager = DatabaseManager(test_database)
        account_manager = AccountManager(db_manager)
        
        account_manager.create_account("Test Account", AccountType.BANK)
        
        with pytest.raises(ValueError):
            account_manager.create_account("Test Account", AccountType.CREDIT)
        
        db_manager.close()
    
    def test_list_accounts(self, test_database):
        """Test listing accounts."""
        db_manager = DatabaseManager(test_database)
        account_manager = AccountManager(db_manager)
        
        account_manager.create_account("Account 1", AccountType.BANK)
        account_manager.create_account("Account 2", AccountType.CREDIT)
        
        accounts = account_manager.list_accounts()
        assert len(accounts) == 2
        
        # Test filtering by type
        bank_accounts = account_manager.list_accounts(AccountType.BANK)
        assert len(bank_accounts) == 1
        assert bank_accounts[0].type == AccountType.BANK
        
        db_manager.close()
    
    def test_update_account(self, test_database):
        """Test updating an account."""
        db_manager = DatabaseManager(test_database)
        account_manager = AccountManager(db_manager)
        
        account = account_manager.create_account("Test Account", AccountType.BANK, 500.0)
        
        updated = account_manager.update_account(
            account_id=account.id,
            name="Updated Account",
            balance=750.0
        )
        
        assert updated.name == "Updated Account"
        assert updated.balance == 750.0
        
        db_manager.close()
    
    def test_calculate_account_balance(self, test_database):
        """Test calculating account balance from transactions."""
        from duplicate_detection import DuplicateDetector
        
        db_manager = DatabaseManager(test_database)
        account_manager = AccountManager(db_manager)
        detector = DuplicateDetector(["date", "description", "amount"])
        
        account = account_manager.create_account("Test Account", AccountType.BANK, 0.0)
        
        # Add transactions
        transactions = [
            {
                "date": datetime(2024, 1, 15),
                "description": "Deposit",
                "amount": 1000.0,
                "category": "Income",
                "account_id": account.id,
                "account": account.name,
                "source_file": "test.csv",
                "duplicate_hash": detector.generate_hash({
                    "date": datetime(2024, 1, 15),
                    "description": "Deposit",
                    "amount": 1000.0
                }),
                "is_transfer": 0
            },
            {
                "date": datetime(2024, 1, 16),
                "description": "Purchase",
                "amount": -50.0,
                "category": "Shopping",
                "account_id": account.id,
                "account": account.name,
                "source_file": "test.csv",
                "duplicate_hash": detector.generate_hash({
                    "date": datetime(2024, 1, 16),
                    "description": "Purchase",
                    "amount": -50.0
                }),
                "is_transfer": 0
            }
        ]
        
        db_manager.insert_transactions(transactions)
        
        balance = account_manager.recalculate_balance(account.id)
        assert balance == 950.0
        
        db_manager.close()


class TestCategorization:
    """Tests for categorization engine."""
    
    def test_categorization_rule_matching(self):
        """Test categorization rule matching."""
        rule = CategorizationRule(
            pattern=r"AMAZON",
            category="Shopping",
            priority=10
        )
        
        assert rule.matches("AMAZON Purchase", None)
        assert rule.matches("amazon marketplace", None)
        assert not rule.matches("Grocery Store", None)
    
    def test_categorization_engine(self):
        """Test categorization engine."""
        engine = CategorizationEngine()
        engine.add_rule("AMAZON", "Shopping", priority=10)
        engine.add_rule("GAS", "Transportation", priority=10)
        
        category1 = engine.categorize("AMAZON Purchase", None)
        assert category1 == "Shopping"
        
        category2 = engine.categorize("Gas Station", None)
        assert category2 == "Transportation"
        
        category3 = engine.categorize("Unknown Transaction", None)
        assert category3 is None
    
    def test_categorization_with_amount_filter(self):
        """Test categorization with amount filters."""
        engine = CategorizationEngine()
        engine.add_rule("DEPOSIT", "Income", priority=10, amount_min=100.0)
        
        # Should match (amount >= 100)
        category1 = engine.categorize("Salary Deposit", 1000.0)
        assert category1 == "Income"
        
        # Should not match (amount < 100)
        category2 = engine.categorize("Small Deposit", 50.0)
        assert category2 is None


class TestEnhancedImport:
    """Tests for enhanced import with account linking."""
    
    def test_detect_account_type_from_filename(self, test_database):
        """Test account type detection from filename."""
        db_manager = DatabaseManager(test_database)
        account_manager = AccountManager(db_manager)
        importer = EnhancedImporter(db_manager, account_manager)
        
        # Test credit card detection
        account_type = importer.detect_account_type_from_filename("credit_card_statement.csv")
        assert account_type == AccountType.CREDIT
        
        # Test investment detection
        account_type = importer.detect_account_type_from_filename("investment_portfolio.csv")
        assert account_type == AccountType.INVESTMENT
        
        # Test bank detection (default)
        account_type = importer.detect_account_type_from_filename("checking_account.csv")
        assert account_type == AccountType.BANK
        
        db_manager.close()
    
    def test_detect_or_create_account(self, test_database):
        """Test account detection/creation during import."""
        db_manager = DatabaseManager(test_database)
        account_manager = AccountManager(db_manager)
        importer = EnhancedImporter(db_manager, account_manager)
        
        # Create account from filename
        account = importer.detect_or_create_account(
            filename="test_checking.csv",
            headers=["Date", "Description", "Amount"]
        )
        
        assert account is not None
        assert "test checking" in account.name.lower() or "test" in account.name.lower()
        assert account.type == AccountType.BANK
        
        db_manager.close()


class TestBudgeting:
    """Tests for budget management."""
    
    def test_create_budget(self, test_database):
        """Test creating a budget."""
        db_manager = DatabaseManager(test_database)
        budget_manager = BudgetManager(db_manager)
        
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)
        
        budget = budget_manager.create_budget(
            category="Groceries",
            allocated_amount=500.0,
            period_start=start_date,
            period_end=end_date
        )
        
        assert budget is not None
        assert budget.category == "Groceries"
        assert budget.allocated_amount == 500.0
        
        db_manager.close()
    
    def test_get_budget_status(self, test_database):
        """Test getting budget status."""
        from duplicate_detection import DuplicateDetector
        
        db_manager = DatabaseManager(test_database)
        budget_manager = BudgetManager(db_manager)
        detector = DuplicateDetector(["date", "description", "amount"])
        
        # Create budget
        budget = budget_manager.create_budget(
            category="Groceries",
            allocated_amount=500.0,
            period_start=date(2024, 1, 1),
            period_end=date(2024, 1, 31)
        )
        
        # Add spending transaction
        account = AccountManager(db_manager).create_account("Test", AccountType.BANK)
        transactions = [{
            "date": datetime(2024, 1, 15),
            "description": "Grocery Store",
            "amount": -100.0,
            "category": "Groceries",
            "account_id": account.id,
            "account": account.name,
            "source_file": "test.csv",
            "duplicate_hash": detector.generate_hash({
                "date": datetime(2024, 1, 15),
                "description": "Grocery Store",
                "amount": -100.0
            }),
            "is_transfer": 0
        }]
        db_manager.insert_transactions(transactions)
        
        # Get budget status
        status = budget_manager.get_budget_status("Groceries", date(2024, 1, 20))
        
        assert status is not None
        assert status.allocated == 500.0
        assert status.spent == 100.0
        assert status.remaining == 400.0
        assert status.percentage_used == 20.0
        
        db_manager.close()

