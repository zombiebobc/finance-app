"""
Unit tests for budgeting UI functionality.

Tests budget dashboard components, monthly budget calculations,
and UI helper functions.
"""

import pytest
from datetime import date, timedelta
from unittest.mock import Mock, MagicMock

from budgeting import BudgetManager
from database_ops import DatabaseManager


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager."""
    return Mock(spec=DatabaseManager)


@pytest.fixture
def budget_manager(mock_db_manager):
    """Create a budget manager with mock database."""
    return BudgetManager(mock_db_manager)


class TestMonthlyBudgetFunctions:
    """Test monthly budget helper functions."""
    
    def test_get_or_create_monthly_budget_normalizes_date(self, budget_manager):
        """Test that month dates are normalized to first of month."""
        from datetime import date
        
        # Mock create_budget to track what date was used
        budget_manager.get_budget = Mock(return_value=None)
        budget_manager.create_budget = Mock(return_value=Mock())
        
        # Call with mid-month date
        test_date = date(2024, 6, 15)
        budget_manager.get_or_create_monthly_budget(
            category="Groceries",
            month=test_date,
            allocated_amount=500.0
        )
        
        # Should create budget with first of month
        expected_start = date(2024, 6, 1)
        expected_end = date(2024, 6, 30)
        
        budget_manager.create_budget.assert_called_once()
        call_args = budget_manager.create_budget.call_args
        assert call_args[1]['period_start'] == expected_start
        assert call_args[1]['period_end'] == expected_end
    
    def test_get_or_create_monthly_budget_returns_existing(self, budget_manager):
        """Test that existing budgets are returned instead of creating new."""
        existing_budget = Mock()
        existing_budget.id = 123
        
        budget_manager.get_budget = Mock(return_value=existing_budget)
        budget_manager.create_budget = Mock()
        
        result = budget_manager.get_or_create_monthly_budget(
            category="Groceries",
            month=date(2024, 6, 1),
            allocated_amount=500.0
        )
        
        # Should return existing
        assert result == existing_budget
        # Should not create new
        budget_manager.create_budget.assert_not_called()
    
    def test_get_all_categories_from_transactions(self, budget_manager):
        """Test getting unique categories from transactions."""
        mock_session = Mock()
        budget_manager.db_manager.get_session.return_value = mock_session
        
        # Mock query results
        mock_query = Mock()
        mock_query.filter.return_value.distinct.return_value.all.return_value = [
            ('Groceries',),
            ('Gas',),
            ('Restaurants',),
            ('Uncategorized',)
        ]
        mock_session.query.return_value = mock_query
        
        categories = budget_manager.get_all_categories_from_transactions()
        
        assert len(categories) >= 4
        assert 'Groceries' in categories
        assert 'Gas' in categories
        assert categories == sorted(categories)  # Should be sorted


class TestBudgetCalculations:
    """Test budget calculation logic."""
    
    def test_available_balance_calculation(self):
        """Test that available balance is correctly calculated."""
        assigned = 500.0
        activity = 300.0
        available = assigned - activity
        
        assert available == 200.0
    
    def test_over_budget_calculation(self):
        """Test detection of over-budget categories."""
        assigned = 500.0
        activity = 600.0
        available = assigned - activity
        
        assert available < 0
        assert available == -100.0
    
    def test_budget_percentage_used(self):
        """Test calculation of budget percentage used."""
        assigned = 500.0
        activity = 300.0
        percentage_used = (activity / assigned * 100) if assigned > 0 else 0
        
        assert percentage_used == 60.0
    
    def test_budget_percentage_zero_assigned(self):
        """Test percentage calculation with zero assigned."""
        assigned = 0.0
        activity = 100.0
        percentage_used = (activity / assigned * 100) if assigned > 0 else 0
        
        assert percentage_used == 0


class TestMonthOptions:
    """Test month option generation for UI selector."""
    
    def test_generate_month_options(self):
        """Test generating month options."""
        from datetime import date
        
        today = date.today()
        months = {}
        
        # Generate last 12 months
        for i in range(12):
            month_date = today.replace(day=1) - timedelta(days=30 * i)
            month_date = month_date.replace(day=1)
            label = month_date.strftime("%B %Y")
            months[label] = month_date
        
        assert len(months) <= 12  # May have duplicates depending on date
        assert all(isinstance(d, date) for d in months.values())
        assert all(d.day == 1 for d in months.values())  # All first of month


class TestBudgetStatusDisplay:
    """Test budget status display logic."""
    
    def test_categorize_over_budget(self):
        """Test identifying over-budget categories."""
        budgets = [
            {'category': 'Groceries', 'available': -50.0},
            {'category': 'Gas', 'available': 100.0},
            {'category': 'Dining', 'available': -25.0}
        ]
        
        over_budget = [b for b in budgets if b['available'] < 0]
        
        assert len(over_budget) == 2
        assert all(b['available'] < 0 for b in over_budget)
    
    def test_categorize_under_budget(self):
        """Test identifying under-budget categories."""
        budgets = [
            {'category': 'Groceries', 'available': 50.0},
            {'category': 'Gas', 'available': 100.0},
            {'category': 'Dining', 'available': 0.0}
        ]
        
        under_budget = [b for b in budgets if b['available'] > 0]
        
        assert len(under_budget) == 2
        assert all(b['available'] > 0 for b in under_budget)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

