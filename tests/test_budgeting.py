"""
Unit tests for budgeting business logic helpers.

Validates category retrieval, budget availability, and overview calculations.
"""

from datetime import date, datetime
from unittest.mock import Mock

import pytest

from budgeting import BudgetManager
from database_ops import DatabaseManager


@pytest.fixture
def mock_db_manager():
    """Provide a mocked database manager instance."""
    return Mock(spec=DatabaseManager)


@pytest.fixture
def budget_manager(mock_db_manager):
    """Return a BudgetManager wired to the mocked database manager."""
    return BudgetManager(mock_db_manager)


class DummyBudget:
    """Simple stand-in for database Budget objects for testing."""

    def __init__(self, budget_id: int, category: str, allocated: float, start: datetime, end: datetime):
        self.id = budget_id
        self.category = category
        self.allocated_amount = allocated
        self.period_start = start
        self.period_end = end


class TestBudgetCategoryHelpers:
    """Tests for category sourcing and availability helpers."""

    def test_get_budget_categories_falls_back_to_config(self, budget_manager, monkeypatch):
        """Ensure configuration fallback is used when transactions provide no categories."""

        monkeypatch.setattr(budget_manager, "get_all_categories_from_transactions", Mock(return_value=[]))
        monkeypatch.setattr(
            BudgetManager,
            "_load_budget_categories_from_config",
            staticmethod(lambda: ["Groceries", "Rent", "Utilities"])
        )

        categories = budget_manager.get_budget_categories()

        assert categories == ["Groceries", "Rent", "Utilities"]

    def test_get_available_categories_excludes_existing(self, budget_manager, monkeypatch):
        """Existing budgets (case-insensitive) should be removed from available list."""

        month = date(2024, 6, 1)
        start_dt = datetime(2024, 6, 1)
        end_dt = datetime(2024, 6, 30, 23, 59, 59)
        existing = [
            DummyBudget(1, "Groceries", 500.0, start_dt, end_dt),
            DummyBudget(2, "rent", 1200.0, start_dt, end_dt),
        ]

        monkeypatch.setattr(budget_manager, "get_monthly_budgets", Mock(return_value=existing))

        available = budget_manager.get_available_categories_for_month(
            month,
            categories=["Groceries", "Rent", "Utilities", "Dining Out"]
        )

        assert available == ["Dining Out", "Utilities"]


class TestBudgetOverview:
    """Tests for budget overview aggregation logic."""

    def test_get_budget_overview_calculates_activity_and_available(self, budget_manager, monkeypatch):
        """Overview should include activity, availability, and usage percentage."""

        month = date(2024, 7, 1)
        start_dt = datetime(2024, 7, 1)
        end_dt = datetime(2024, 7, 31, 23, 59, 59)
        budgets = [
            DummyBudget(10, "Dining", 400.0, start_dt, end_dt),
        ]

        monkeypatch.setattr(budget_manager, "get_monthly_budgets", Mock(return_value=budgets))
        monkeypatch.setattr(budget_manager, "calculate_category_spending", Mock(return_value=150.0))

        overview = budget_manager.get_budget_overview(month)

        assert len(overview) == 1
        entry = overview[0]
        assert entry["category"] == "Dining"
        assert entry["activity"] == 150.0
        assert entry["available"] == pytest.approx(250.0)
        assert entry["budget_used_pct"] == pytest.approx(37.5)

