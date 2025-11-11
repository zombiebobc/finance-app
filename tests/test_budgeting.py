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
        monkeypatch.setattr(
            budget_manager,
            "get_activity_by_category",
            Mock(return_value={"dining": 150.0})
        )

        overview = budget_manager.get_budget_overview(month)

        assert len(overview) == 1
        entry = overview[0]
        assert entry["category"] == "Dining"
        assert entry["activity"] == 150.0
        assert entry["available"] == pytest.approx(250.0)
        assert entry["budget_used_pct"] == pytest.approx(37.5)
        assert entry["canonical_key"] == "dining"
    
    def test_filter_budget_overview_excludes_zero_assigned(self):
        """filter_budget_overview should drop entries with zero assigned when strict."""
        overview = [
            {"category": "Groceries", "assigned": 500.0},
            {"category": "Rent", "assigned": 0.0},
            {"category": "Dining", "assigned": 50.0},
        ]
        
        filtered = BudgetManager.filter_budget_overview(overview, min_assigned=0.0, strict=True)
        
        assert len(filtered) == 2
        assert all(entry["assigned"] > 0 for entry in filtered)
        assert {entry["category"] for entry in filtered} == {"Groceries", "Dining"}
    
    def test_calculate_budget_summary_handles_empty(self):
        """Summary should return zeros when overview is empty."""
        summary = BudgetManager.calculate_budget_summary([])
        assert summary["total_assigned"] == 0.0
        assert summary["total_activity"] == 0.0
        assert summary["total_available"] == 0.0
        assert summary["budget_used_pct"] == 0.0
    
    def test_calculate_budget_summary_totals(self):
        """Summary should sum assigned, activity, and available correctly."""
        overview = [
            {"assigned": 100.0, "activity": 70.0, "available": 30.0},
            {"assigned": 200.0, "activity": 150.0, "available": 50.0},
        ]
        
        summary = BudgetManager.calculate_budget_summary(overview)
        assert summary["total_assigned"] == pytest.approx(300.0)
        assert summary["total_activity"] == pytest.approx(220.0)
        assert summary["total_available"] == pytest.approx(80.0)
        assert summary["budget_used_pct"] == pytest.approx(73.3333333333, rel=1e-6)
    
    def test_calculate_unassigned(self):
        """calculate_unassigned should subtract assigned from income."""
        assert BudgetManager.calculate_unassigned(1000.0, 750.0) == pytest.approx(250.0)
        assert BudgetManager.calculate_unassigned(500.0, 600.0) == pytest.approx(-100.0)
    
    def test_calculate_projected_balance(self):
        """Projected balance should use linear extrapolation."""
        projected = BudgetManager.calculate_projected_balance(
            current_balances=2000.0,
            days_left=10,
            avg_daily_income=150.0,
            avg_daily_spend=100.0
        )
        assert projected == pytest.approx(2000.0 + 10 * (150.0 - 100.0))
    
    def test_get_health_tips_generates_messages(self):
        """Tips should reflect snapshot conditions."""
        tips = BudgetManager.get_health_tips({
            "unassigned_funds": -50.0,
            "available_total": -10.0,
            "assigned_total": 500.0,
            "budget_utilization_pct": 95.0,
            "projected_balance": -20.0,
        })
        assert any("over-assigned" in tip.lower() for tip in tips)
        assert any("overspent" in tip.lower() or "spending" in tip.lower() for tip in tips)

    def test_get_activity_by_category_queries_database(self, budget_manager):
        """get_activity_by_category should aggregate negative amounts and return positives."""
        mock_session = Mock()
        query = Mock()
        group = Mock()
        group.all.return_value = [
            ("groceries", -120.0),
            ("rent", -1000.0),
            (None, -50.0),
        ]
        query.filter.return_value = query
        query.group_by.return_value = group
        mock_session.query.return_value = query
        budget_manager.db_manager.get_session.return_value = mock_session

        activity = budget_manager.get_activity_by_category(
            period_start=date(2024, 6, 1),
            period_end=date(2024, 6, 30),
            categories=["Groceries", "Rent"]
        )

        assert activity["groceries"] == pytest.approx(120.0)
        assert activity["rent"] == pytest.approx(1000.0)
        assert "none" not in activity
        mock_session.close.assert_called_once()

