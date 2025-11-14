"""
Budgeting module for YNAB-like budget management.

This module provides budget envelope functionality, allowing users to
allocate funds to categories and track spending against budgets.
"""

import logging
from typing import List, Optional, Dict, Any, Tuple, Set
from datetime import UTC, datetime, date, timedelta
from dataclasses import dataclass

from database_ops import DatabaseManager, Budget, Transaction, IncomeOverride, Account
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, and_, extract
from exceptions import BudgetError

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class BudgetStatus:
    """
    Status of a budget category.
    
    Attributes:
        category: Category name
        allocated: Amount allocated to category
        spent: Amount spent in category
        remaining: Remaining budget (allocated - spent)
        percentage_used: Percentage of budget used
    """
    category: str
    allocated: float
    spent: float
    remaining: float
    percentage_used: float


class BudgetManager:
    """
    Manages budgets and budget envelopes (YNAB-style).
    
    Provides functionality to allocate funds to categories and track
    spending against those allocations.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the budget manager.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
        logger.info("Budget manager initialized")
    
    @staticmethod
    def _normalize_category(category: Optional[str]) -> str:
        """
        Normalize category names by stripping whitespace.
        
        Args:
            category: Raw category name.
        
        Returns:
            Normalized category string (empty string if None).
        """
        if category is None:
            return ""
        return category.strip()
    
    @staticmethod
    def _category_key(category: Optional[str]) -> str:
        """
        Generate a case-insensitive key for category comparisons.
        
        Args:
            category: Category name.
        
        Returns:
            Lowercase category key.
        """
        return BudgetManager._normalize_category(category).lower()
    
    @staticmethod
    def _load_budget_categories_from_config() -> List[str]:
        """
        Load fallback budget categories from configuration.
        
        Returns:
            List of category names from config, or empty list.
        """
        try:
            from config_manager import load_config  # Lazy import to avoid circular dependency
        except ImportError:
            logger.debug("config_manager not available; skipping budget category fallback.")
            return []
        
        categories_config = load_config().get("budget_categories", [])
        if not isinstance(categories_config, list):
            logger.warning("Config value 'budget_categories' is not a list. Ignoring fallback categories.")
            return []
        
        unique: Dict[str, str] = {}
        for raw in categories_config:
            normalized = BudgetManager._normalize_category(str(raw))
            if not normalized:
                continue
            key = BudgetManager._category_key(normalized)
            if key and key not in unique:
                unique[key] = normalized
        
        canonical_labels, _, _ = BudgetManager._load_budget_category_aliases()
        for canonical_key, label in canonical_labels.items():
            if canonical_key not in unique and label:
                unique[canonical_key] = label
        
        return sorted(unique.values(), key=str.casefold)
    
    @staticmethod
    def _load_budget_category_aliases() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, Set[str]]]:
        """
        Load budget category alias mappings from configuration.
        
        Returns:
            Tuple of (canonical_labels, alias_lookup, canonical_aliases)
        """
        try:
            from config_manager import load_config  # Lazy import to avoid circular dependency
        except ImportError:
            logger.debug("config_manager not available; skipping budget category aliases.")
            return {}, {}, {}
        
        config = load_config()
        alias_config = config.get("budget_category_aliases", {}) or {}
        
        canonical_labels: Dict[str, str] = {}
        alias_lookup: Dict[str, str] = {}
        canonical_aliases: Dict[str, Set[str]] = {}
        
        for canonical, aliases in alias_config.items():
            canonical_norm = BudgetManager._normalize_category(str(canonical))
            if not canonical_norm:
                continue
            canonical_key = BudgetManager._category_key(canonical_norm)
            canonical_labels[canonical_key] = canonical_norm
            alias_lookup[canonical_key] = canonical_key
            canonical_aliases.setdefault(canonical_key, set())
            
            for alias in aliases or []:
                alias_norm = BudgetManager._normalize_category(str(alias))
                if not alias_norm:
                    continue
                alias_key = BudgetManager._category_key(alias_norm)
                alias_lookup[alias_key] = canonical_key
                canonical_aliases.setdefault(canonical_key, set()).add(alias_key)
        
        return canonical_labels, alias_lookup, canonical_aliases
    
    @staticmethod
    def _load_income_categories_from_config() -> List[str]:
        """
        Load configured income categories for filtering income calculations.
        
        Returns:
            List of normalized income category names.
        """
        try:
            from config_manager import load_config  # Lazy import
        except ImportError:
            logger.debug("config_manager not available; skipping income category config.")
            return []
        
        config = load_config()
        categories = config.get("database", {}).get("income_categories", [])
        if not isinstance(categories, list):
            logger.warning("Config value 'income_categories' should be a list; ignoring.")
            return []
        
        normalized = []
        for raw in categories:
            norm = BudgetManager._normalize_category(str(raw))
            if norm:
                normalized.append(norm.lower())
        return normalized
    
    @staticmethod
    def _show_projections_enabled() -> bool:
        """Return True if projections should be displayed according to config."""
        try:
            from config_manager import load_config  # Lazy import
        except ImportError:
            return True
        config = load_config()
        return config.get("show_projections", True)
    
    @staticmethod
    def get_month_period(month: date) -> Tuple[date, date]:
        """
        Get the first and last day for the provided month.
        
        Args:
            month: Date within the desired month (only month/year are used).
        
        Returns:
            Tuple of (period_start, period_end).
        """
        period_start = month.replace(day=1)
        if period_start.month == 12:
            period_end = period_start.replace(year=period_start.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            period_end = period_start.replace(month=period_start.month + 1, day=1) - timedelta(days=1)
        return period_start, period_end
    
    def create_budget(
        self,
        category: str,
        allocated_amount: float,
        period_start: date,
        period_end: date
    ) -> Optional[Budget]:
        """
        Create a budget for a category.
        
        Args:
            category: Category name
            allocated_amount: Amount allocated to this category
            period_start: Start date of budget period
            period_end: End date of budget period
        
        Returns:
            Created Budget object, or None if creation failed
        """
        session = self.db_manager.get_session()
        
        try:
            normalized_category = self._normalize_category(category)
            if not normalized_category:
                logger.error("Cannot create budget without a category name.")
                return None
            
            category_key = self._category_key(normalized_category)
            period_start_dt = datetime.combine(period_start, datetime.min.time())
            period_end_dt = datetime.combine(period_end, datetime.max.time())
            
            # Check for overlapping budgets
            existing = session.query(Budget).filter(
                func.lower(Budget.category) == category_key,
                Budget.period_start <= period_end_dt,
                Budget.period_end >= period_start_dt
            ).first()
            
            if existing:
                logger.warning(
                    f"Budget already exists for category '{category}' "
                    f"overlapping period {period_start} to {period_end}"
                )
                # Update existing instead
                existing.category = normalized_category
                existing.allocated_amount = allocated_amount
                existing.period_start = period_start_dt
                existing.period_end = period_end_dt
                existing.updated_at = datetime.now(UTC)
                session.commit()
                session.refresh(existing)
                session.expunge(existing)
                return existing
            
            budget = Budget(
                category=normalized_category,
                allocated_amount=allocated_amount,
                period_start=period_start_dt,
                period_end=period_end_dt
            )
            
            session.add(budget)
            session.commit()
            session.refresh(budget)
            session.expunge(budget)
            
            logger.info(f"Created budget for '{category}': ${allocated_amount} ({period_start} to {period_end})")
            return budget
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to create budget: {e}")
            return None
        finally:
            session.close()
    
    def get_budget(
        self,
        category: str,
        period_date: Optional[date] = None
    ) -> Optional[Budget]:
        """
        Get budget for a category.
        
        Args:
            category: Category name
            period_date: Date to check (defaults to today)
        
        Returns:
            Budget object or None if not found
        """
        if period_date is None:
            period_date = date.today()
        
        session = self.db_manager.get_session()
        try:
            period_datetime = datetime.combine(period_date, datetime.min.time())
            normalized_category = self._normalize_category(category)
            if not normalized_category:
                return None
            category_key = self._category_key(normalized_category)
            
            candidates = session.query(Budget).filter(
                Budget.period_start <= period_datetime,
                Budget.period_end >= period_datetime
            ).all()
            
            for budget in candidates:
                existing_key = self._category_key(self._normalize_category(budget.category))
                if existing_key == category_key:
                    session.expunge(budget)
                    return budget
            return None
        except SQLAlchemyError as e:
            logger.error(f"Failed to get budget: {e}")
            return None
        finally:
            session.close()
    
    def get_all_budgets(self, period_date: Optional[date] = None) -> List[Budget]:
        """
        Get all active budgets.
        
        Args:
            period_date: Date to check (defaults to today)
        
        Returns:
            List of Budget objects
        """
        if period_date is None:
            period_date = date.today()
        
        session = self.db_manager.get_session()
        try:
            period_datetime = datetime.combine(period_date, datetime.min.time())
            
            budgets = session.query(Budget).filter(
                Budget.period_start <= period_datetime,
                Budget.period_end >= period_datetime
            ).order_by(Budget.category).all()
            
            for budget in budgets:
                session.expunge(budget)
            return budgets
        except SQLAlchemyError as e:
            logger.error(f"Failed to get budgets: {e}")
            return []
        finally:
            session.close()
    
    def calculate_category_spending(
        self,
        category: str,
        period_start: Optional[date] = None,
        period_end: Optional[date] = None
    ) -> float:
        """
        Calculate total spending for a category in a period.
        
        Legacy wrapper retained for backward compatibility.
        """
        if period_start is None:
            period_start = date.today().replace(day=1)
        if period_end is None:
            next_month = period_start.replace(day=28) + timedelta(days=4)
            period_end = next_month - timedelta(days=next_month.day)
        
        activity_map = self.get_activity_by_category(period_start, period_end, [category])
        category_key = self._category_key(self._normalize_category(category))
        return activity_map.get(category_key, 0.0)
    
    def get_activity_by_category(
        self,
        period_start: date,
        period_end: date,
        categories: Optional[List[str]] = None
    ) -> Dict[str, float]:
        """
        Return absolute spending totals grouped by canonical category key for the period.
        """
        session = self.db_manager.get_session()
        try:
            canonical_labels, alias_lookup, _ = self._load_budget_category_aliases()
            
            start_datetime = datetime.combine(period_start, datetime.min.time())
            end_datetime = datetime.combine(period_end, datetime.max.time())
            
            category_expr = func.lower(func.trim(func.decrypt_text(Transaction.category)))
            amount_expr = func.decrypt_numeric(Transaction.amount)
            query = session.query(
                category_expr.label("category_key"),
                func.sum(amount_expr).label("total")
            ).filter(
                Transaction.date >= start_datetime,
                Transaction.date <= end_datetime,
                Transaction.is_transfer == 0,
                Transaction.amount < 0
            )
            
            if categories:
                category_keys = {
                    self._category_key(self._normalize_category(cat))
                    for cat in categories
                    if self._normalize_category(cat)
                }
                mapped_keys = {alias_lookup.get(k, k) for k in category_keys}
                include_keys = category_keys.union(mapped_keys)
                if include_keys:
                    query = query.filter(category_expr.in_(include_keys))
            
            results = query.group_by("category_key").all()
            activity_map: Dict[str, float] = {}
            for key, total in results:
                if not key:
                    continue
                canonical_key = alias_lookup.get(key, key)
                activity_map[canonical_key] = activity_map.get(canonical_key, 0.0) + abs(total or 0.0)
            
            # ensure canonical keys exist even if zero transactions
            if categories:
                for cat in categories:
                    normalized = self._normalize_category(cat)
                    canonical_key = alias_lookup.get(self._category_key(normalized), self._category_key(normalized))
                    activity_map.setdefault(canonical_key, 0.0)
            else:
                for canonical_key in canonical_labels:
                    activity_map.setdefault(canonical_key, 0.0)
            
            return activity_map
        except SQLAlchemyError as exc:
            logger.error("Failed to calculate activity by category: %s", exc)
            return {}
        finally:
            session.close()
    
    def _apply_income_category_filter(self, query):
        """Apply configured income categories to the provided query if defined."""
        categories = self._load_income_categories_from_config()
        if categories:
            query = query.filter(
                Transaction.category.isnot(None),
                func.lower(func.decrypt_text(Transaction.category)).in_(categories)
            )
        return query
    
    def _sum_transactions(
        self,
        period_start: date,
        period_end: date,
        positive_only: bool = False,
        negative_only: bool = False
    ) -> float:
        """
        Sum transactions for the given period. Expenses are returned as positive values.
        """
        session = self.db_manager.get_session()
        try:
            start_datetime = datetime.combine(period_start, datetime.min.time())
            end_datetime = datetime.combine(period_end, datetime.max.time())
            
            amount_expr = func.decrypt_numeric(Transaction.amount)
            query = session.query(func.sum(amount_expr)).filter(
                Transaction.date >= start_datetime,
                Transaction.date <= end_datetime,
                Transaction.is_transfer == 0
            )
            
            if positive_only and negative_only:
                raise BudgetError(
                    "positive_only and negative_only cannot both be True",
                    details={"operation": "get_budget_status"}
                )
            
            if positive_only:
                query = query.filter(Transaction.amount > 0)
                query = self._apply_income_category_filter(query)
            elif negative_only:
                query = query.filter(Transaction.amount < 0)
            
            total = query.scalar() or 0.0
            if negative_only:
                total = abs(total)
            return float(total)
        except SQLAlchemyError as exc:
            logger.error("Failed to sum transactions: %s", exc)
            return 0.0
        finally:
            session.close()
    
    def get_income_override(self, period_start: date) -> Optional[IncomeOverride]:
        """Return saved income override for the provided period, if any."""
        session = self.db_manager.get_session()
        try:
            override = session.query(IncomeOverride).filter(
                IncomeOverride.period_start == period_start
            ).first()
            if override:
                session.expunge(override)
            return override
        except SQLAlchemyError as exc:
            logger.error("Failed to fetch income override: %s", exc)
            return None
        finally:
            session.close()
    
    def upsert_income_override(
        self,
        period_start: date,
        period_end: date,
        amount: float,
        notes: Optional[str] = None
    ) -> Optional[IncomeOverride]:
        """Create or update a monthly income override."""
        session = self.db_manager.get_session()
        try:
            override = session.query(IncomeOverride).filter(
                IncomeOverride.period_start == period_start
            ).first()
            
            if override:
                override.override_amount = amount
                override.period_end = period_end
                override.notes = notes
                override.updated_at = datetime.now(UTC)
            else:
                override = IncomeOverride(
                    period_start=period_start,
                    period_end=period_end,
                    override_amount=amount,
                    notes=notes
                )
                session.add(override)
            
            session.commit()
            session.refresh(override)
            session.expunge(override)
            logger.info("Income override saved for %s: %s", period_start, amount)
            return override
        except SQLAlchemyError as exc:
            session.rollback()
            logger.error("Failed to upsert income override: %s", exc)
            return None
        finally:
            session.close()
    
    def delete_income_override(self, period_start: date) -> bool:
        """Delete an existing income override for the given period."""
        session = self.db_manager.get_session()
        try:
            deleted = session.query(IncomeOverride).filter(
                IncomeOverride.period_start == period_start
            ).delete()
            if deleted:
                session.commit()
                logger.info("Income override cleared for %s", period_start)
                return True
            return False
        except SQLAlchemyError as exc:
            session.rollback()
            logger.error("Failed to delete income override: %s", exc)
            return False
        finally:
            session.close()
    
    def calculate_historical_income_average(
        self,
        period_start: date,
        months: int = 3
    ) -> float:
        """
        Calculate average income for the previous N full months.
        """
        totals: List[float] = []
        month_start = period_start.replace(day=1)
        
        for _ in range(months):
            prev_month_end = month_start - timedelta(days=1)
            prev_month_start = prev_month_end.replace(day=1)
            total = self._sum_transactions(prev_month_start, prev_month_end, positive_only=True)
            if total > 0:
                totals.append(total)
            month_start = prev_month_start
        
        if not totals:
            return 0.0
        return sum(totals) / len(totals)
    
    def calculate_monthly_income(
        self,
        period_start: date,
        period_end: date,
        fallback_months: int = 3
    ) -> Dict[str, Any]:
        """
        Determine income for the month with override/historical fallback.
        """
        override = self.get_income_override(period_start)
        if override:
            return {
                "amount": float(override.override_amount),
                "source": "override",
                "override": override
            }
        
        actual = self._sum_transactions(period_start, period_end, positive_only=True)
        if actual > 0:
            return {"amount": actual, "source": "actual", "override": None}
        
        historical_avg = self.calculate_historical_income_average(period_start, fallback_months)
        return {"amount": historical_avg, "source": "historical", "override": None}
    
    def get_account_balance_total(self) -> float:
        """Return the sum of all account balances."""
        session = self.db_manager.get_session()
        try:
            total = session.query(func.sum(Account.balance)).scalar() or 0.0
            return float(total)
        except SQLAlchemyError as exc:
            logger.error("Failed to fetch account balances: %s", exc)
            return 0.0
        finally:
            session.close()
    
    def calculate_daily_income_expense(
        self,
        period_start: date,
        current_date: date
    ) -> Dict[str, float]:
        """
        Calculate daily average income and expense for the month to date.
        """
        if current_date < period_start:
            return {
                "days_elapsed": 0,
                "income_to_date": 0.0,
                "spend_to_date": 0.0,
                "avg_daily_income": 0.0,
                "avg_daily_spend": 0.0
            }
        
        income_to_date = self._sum_transactions(period_start, current_date, positive_only=True)
        spend_to_date = self._sum_transactions(period_start, current_date, negative_only=True)
        days_elapsed = (current_date - period_start).days + 1
        days_elapsed = max(days_elapsed, 1)
        
        return {
            "days_elapsed": days_elapsed,
            "income_to_date": income_to_date,
            "spend_to_date": spend_to_date,
            "avg_daily_income": income_to_date / days_elapsed if days_elapsed else 0.0,
            "avg_daily_spend": spend_to_date / days_elapsed if days_elapsed else 0.0
        }
    
    @staticmethod
    def calculate_unassigned(income: float, assigned: float) -> float:
        """Return unassigned funds (income minus assigned)."""
        return income - assigned
    
    @staticmethod
    def calculate_projected_balance(
        current_balances: float,
        days_left: int,
        avg_daily_income: float,
        avg_daily_spend: float
    ) -> float:
        """Simple projection of end-of-month balance."""
        return current_balances + days_left * (avg_daily_income - avg_daily_spend)
    
    @staticmethod
    def get_health_tips(snapshot: Dict[str, Any]) -> List[str]:
        """Generate budget tips based on snapshot metrics."""
        tips: List[str] = []
        if snapshot["unassigned_funds"] > 0:
            tips.append("Allocate remaining unassigned dollars to savings or priority goals (YNAB Rule 1).")
        elif snapshot["unassigned_funds"] < 0:
            tips.append("You've over-assigned your income. Move money from lower-priority categories.")
        
        if snapshot["available_total"] < 0:
            tips.append("Spending exceeds your assignments. Trim or move funds from other categories.")
        elif snapshot["available_total"] == 0 and snapshot["assigned_total"] > 0:
            tips.append("All assigned funds are spoken for—consider building a buffer for next month.")
        
        if snapshot["budget_utilization_pct"] >= 90:
            tips.append("Budget utilization is high. Pause discretionary spending to stay on track.")
        
        projected = snapshot.get("projected_balance")
        if projected is not None and projected < 0:
            tips.append("Projected balance is negative. Boost income or cut spending to avoid a cash shortfall.")
        
        if not tips:
            tips.append("Great job! Keep aging your money and planning for future expenses.")
        return tips
    
    @staticmethod
    def get_health_alerts(snapshot: Dict[str, Any]) -> List[str]:
        """High-priority alerts derived from snapshot metrics."""
        alerts: List[str] = []
        if snapshot["unassigned_funds"] < 0:
            alerts.append("Unassigned funds are negative—rebalance your budget to match income.")
        if snapshot["available_total"] < 0:
            alerts.append("Remaining available is negative—at least one category is overspent.")
        projected = snapshot.get("projected_balance")
        if projected is not None and projected < 0:
            alerts.append("Projected end-of-month balance is negative.")
        return alerts
    
    def build_financial_snapshot(
        self,
        period_start: date,
        period_end: date,
        active_budgets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Aggregate metrics for the Financial Health Snapshot."""
        assigned_total = sum(float(item.get("assigned", 0.0)) for item in active_budgets)
        spent_total = sum(float(item.get("activity", 0.0)) for item in active_budgets)
        available_total = sum(float(item.get("available", 0.0)) for item in active_budgets)
        utilization_pct = (spent_total / assigned_total * 100.0) if assigned_total > 0 else 0.0
        
        income_info = self.calculate_monthly_income(period_start, period_end)
        income_total = float(income_info["amount"])
        unassigned = self.calculate_unassigned(income_total, assigned_total)
        
        today = date.today()
        current_date = min(max(today, period_start), period_end)
        days_in_period = (period_end - period_start).days + 1
        days_left = max((period_end - current_date).days, 0)
        
        daily_stats = self.calculate_daily_income_expense(period_start, current_date)
        show_projections = self._show_projections_enabled()
        
        if daily_stats["days_elapsed"] == 0:
            avg_income = income_total / days_in_period if days_in_period else 0.0
            avg_spend = spent_total / days_in_period if days_in_period else 0.0
        else:
            avg_income = daily_stats["avg_daily_income"]
            avg_spend = daily_stats["avg_daily_spend"]
            if avg_income == 0 and income_total > 0:
                avg_income = income_total / daily_stats["days_elapsed"]
        
        if income_total == 0:
            historical_avg_income = self.calculate_historical_income_average(period_start)
            if historical_avg_income > 0 and days_in_period:
                avg_income = max(avg_income, historical_avg_income / days_in_period)
        
        current_balances = self.get_account_balance_total()
        projected_balance = None
        if show_projections:
            projected_balance = self.calculate_projected_balance(
                current_balances=current_balances,
                days_left=days_left,
                avg_daily_income=avg_income,
                avg_daily_spend=avg_spend
            )
        
        snapshot_base = {
            "unassigned_funds": unassigned,
            "available_total": available_total,
            "assigned_total": assigned_total,
            "budget_utilization_pct": utilization_pct,
            "projected_balance": projected_balance,
        }
        
        snapshot = {
            "income_total": income_total,
            "income_source": income_info["source"],
            "override": income_info["override"],
            "assigned_total": assigned_total,
            "spent_total": spent_total,
            "available_total": available_total,
            "unassigned_funds": unassigned,
            "budget_utilization_pct": utilization_pct,
            "current_balances": current_balances,
            "avg_daily_income": avg_income,
            "avg_daily_spend": avg_spend,
            "days_left": days_left,
            "days_in_period": days_in_period,
            "projected_balance": projected_balance,
            "alerts": self.get_health_alerts(snapshot_base),
            "tips": self.get_health_tips(snapshot_base),
            "show_projections": show_projections,
        }
        return snapshot
    
    def get_budget_status(
        self,
        category: str,
        period_date: Optional[date] = None
    ) -> Optional[BudgetStatus]:
        """
        Get budget status for a category.
        
        Args:
            category: Category name
            period_date: Date to check (defaults to today)
        
        Returns:
            BudgetStatus object or None if no budget exists
        """
        budget = self.get_budget(category, period_date)
        if not budget:
            return None
        
        # Calculate spending for the budget period
        period_start = budget.period_start.date()
        period_end = budget.period_end.date()
        spent = self.calculate_category_spending(category, period_start, period_end)
        
        remaining = budget.allocated_amount - spent
        percentage_used = (spent / budget.allocated_amount * 100) if budget.allocated_amount > 0 else 0.0
        
        return BudgetStatus(
            category=category,
            allocated=budget.allocated_amount,
            spent=spent,
            remaining=remaining,
            percentage_used=percentage_used
        )
    
    def get_all_budget_statuses(self, period_date: Optional[date] = None) -> List[BudgetStatus]:
        """
        Get budget status for all categories with budgets.
        
        Args:
            period_date: Date to check (defaults to today)
        
        Returns:
            List of BudgetStatus objects
        """
        budgets = self.get_all_budgets(period_date)
        statuses = []
        
        for budget in budgets:
            status = self.get_budget_status(budget.category, period_date)
            if status:
                statuses.append(status)
        
        return statuses
    
    def update_budget(
        self,
        budget_id: int,
        allocated_amount: Optional[float] = None,
        period_start: Optional[date] = None,
        period_end: Optional[date] = None
    ) -> Optional[Budget]:
        """
        Update a budget.
        
        Args:
            budget_id: Budget ID
            allocated_amount: New allocated amount (optional)
            period_start: New period start date (optional)
            period_end: New period end date (optional)
        
        Returns:
            Updated Budget object or None if update failed
        """
        session = self.db_manager.get_session()
        
        try:
            budget = session.query(Budget).filter(Budget.id == budget_id).first()
            if not budget:
                logger.warning(f"Budget {budget_id} not found")
                return None
            
            if allocated_amount is not None:
                budget.allocated_amount = allocated_amount
            if period_start is not None:
                budget.period_start = datetime.combine(period_start, datetime.min.time())
            if period_end is not None:
                budget.period_end = datetime.combine(period_end, datetime.max.time())
            
            budget.updated_at = datetime.now(UTC)
            session.commit()
            session.refresh(budget)
            session.expunge(budget)
            
            logger.info(f"Updated budget {budget_id}")
            return budget
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to update budget: {e}")
            return None
        finally:
            session.close()
    
    def delete_budget(self, budget_id: int) -> bool:
        """
        Delete a budget.
        
        Args:
            budget_id: Budget ID
        
        Returns:
            True if deletion succeeded, False otherwise
        """
        session = self.db_manager.get_session()
        
        try:
            budget = session.query(Budget).filter(Budget.id == budget_id).first()
            if not budget:
                logger.warning(f"Budget {budget_id} not found")
                return False
            
            session.delete(budget)
            session.commit()
            
            logger.info(f"Deleted budget {budget_id}")
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Failed to delete budget: {e}")
            return False
        finally:
            session.close()
    
    def get_total_allocated(self, period_date: Optional[date] = None) -> float:
        """
        Get total amount allocated across all budgets.
        
        Args:
            period_date: Date to check (defaults to today)
        
        Returns:
            Total allocated amount
        """
        budgets = self.get_all_budgets(period_date)
        return sum(budget.allocated_amount for budget in budgets)
    
    def get_total_spent(self, period_date: Optional[date] = None) -> float:
        """
        Get total amount spent across all budgeted categories.
        
        Args:
            period_date: Date to check (defaults to today)
        
        Returns:
            Total spent amount
        """
        budgets = self.get_all_budgets(period_date)
        total = 0.0
        
        for budget in budgets:
            period_start = budget.period_start.date()
            period_end = budget.period_end.date()
            total += self.calculate_category_spending(budget.category, period_start, period_end)
        
        return total
    
    def get_all_categories_from_transactions(self) -> List[str]:
        """
        Get all unique categories from transactions for budget setup.
        
        Returns:
            List of unique category names
        """
        session = self.db_manager.get_session()
        
        try:
            canonical_labels, alias_lookup, _ = self._load_budget_category_aliases()
            
            query = session.query(
                func.trim(Transaction.category)
            )
            query = query.filter(
                Transaction.category.isnot(None),
                func.trim(Transaction.category) != "",
                Transaction.is_transfer == 0
            )
            query = query.distinct()
            raw_categories = query.all()
            
            canonical_set: Dict[str, str] = {}
            for (category,) in raw_categories:
                normalized = self._normalize_category(category)
                if not normalized:
                    continue
                key = self._category_key(normalized)
                if key == "transfer":
                    continue
                canonical_key = alias_lookup.get(key, key)
                if canonical_key not in canonical_set:
                    canonical_set[canonical_key] = canonical_labels.get(canonical_key, normalized)
            
            # ensure canonical labels from config are included even without transactions
            for canonical_key, label in canonical_labels.items():
                canonical_set.setdefault(canonical_key, label)
            
            category_list = sorted(canonical_set.values(), key=str.casefold)
            logger.info(f"Found {len(category_list)} unique categories")
            return category_list
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get categories: {e}")
            return []
        finally:
            session.close()
    
    def get_or_create_monthly_budget(
        self,
        category: str,
        month: date,
        allocated_amount: float = 0.0
    ) -> Optional[Budget]:
        """
        Get or create a budget for a specific category and month.
        
        Args:
            category: Category name
            month: Month date (will be normalized to first of month)
            allocated_amount: Amount to allocate if creating new budget
        
        Returns:
            Budget object or None if error
        """
        # Normalize to first of month
        period_start = month.replace(day=1)
        
        # Get last day of month
        if period_start.month == 12:
            period_end = period_start.replace(year=period_start.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            period_end = period_start.replace(month=period_start.month + 1, day=1) - timedelta(days=1)
        
        # Try to get existing budget
        existing = self.get_budget(category, period_start)
        if existing:
            return existing
        
        # Create new budget
        return self.create_budget(
            category=category,
            allocated_amount=allocated_amount,
            period_start=period_start,
            period_end=period_end
        )
    
    def get_budget_categories(self) -> List[str]:
        """
        Retrieve budget categories from transactions or configuration fallback.
        
        Returns:
            Sorted list of distinct categories.
        """
        categories = self.get_all_categories_from_transactions()
        if categories:
            return categories
        fallback = self._load_budget_categories_from_config()
        if fallback:
            logger.info("Using fallback budget categories from configuration.")
        return fallback
    
    def get_available_categories_for_month(
        self,
        month: date,
        categories: Optional[List[str]] = None
    ) -> List[str]:
        """
        Get categories that do not yet have a budget for the specified month.
        
        Args:
            month: Month to evaluate.
            categories: Optional list of categories to evaluate. When omitted,
                categories will be fetched via get_budget_categories.
        
        Returns:
            Sorted list of available category names.
        """
        _, alias_lookup, _ = self._load_budget_category_aliases()
        all_categories = categories if categories is not None else self.get_budget_categories()
        if not all_categories:
            return []
        
        existing_budgets = self.get_monthly_budgets(month)
        existing_keys: Set[str] = {
            alias_lookup.get(self._category_key(self._normalize_category(budget.category)), self._category_key(self._normalize_category(budget.category)))
            for budget in existing_budgets
        }
        
        available = [
            category for category in all_categories
            if alias_lookup.get(self._category_key(self._normalize_category(category)), self._category_key(self._normalize_category(category))) not in existing_keys
        ]
        return sorted(available, key=str.casefold)
    
    def get_monthly_budgets(self, month: date) -> List[Budget]:
        """
        Retrieve budgets for the specified month.
        
        Args:
            month: Month to retrieve budgets for.
        
        Returns:
            List of Budget objects.
        """
        period_start, period_end = self.get_month_period(month)
        start_dt = datetime.combine(period_start, datetime.min.time())
        end_dt = datetime.combine(period_end, datetime.max.time())
        
        session = self.db_manager.get_session()
        try:
            budgets = session.query(Budget).filter(
                Budget.period_start == start_dt,
                Budget.period_end == end_dt
            ).order_by(func.lower(Budget.category)).all()
            
            for budget in budgets:
                session.expunge(budget)
            return budgets
        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch monthly budgets: {e}")
            return []
        finally:
            session.close()
    
    def get_budget_overview(self, month: date) -> List[Dict[str, Any]]:
        """
        Build budget overview data for the UI.
        
        Args:
            month: Month to summarize.
        
        Returns:
            List of dictionaries containing budget summary fields.
        """
        budgets = self.get_monthly_budgets(month)
        overview: List[Dict[str, Any]] = []
        if not budgets:
            return overview
        
        period_start = budgets[0].period_start.date()
        period_end = budgets[0].period_end.date()
        canonical_labels, alias_lookup, _ = self._load_budget_category_aliases()
        activity_map = self.get_activity_by_category(
            period_start=period_start,
            period_end=period_end,
            categories=[budget.category for budget in budgets]
        )
        
        for budget in budgets:
            period_start = budget.period_start.date()
            period_end = budget.period_end.date()
            normalized = self._normalize_category(budget.category)
            category_key = self._category_key(normalized)
            canonical_key = alias_lookup.get(category_key, category_key)
            display_label = canonical_labels.get(canonical_key, normalized)
            activity = activity_map.get(canonical_key, 0.0)
            available = budget.allocated_amount - activity
            used_pct = (activity / budget.allocated_amount * 100) if budget.allocated_amount > 0 else 0.0
            
            overview.append({
                "id": budget.id,
                "category": display_label,
                "canonical_key": canonical_key,
                "assigned": budget.allocated_amount,
                "activity": activity,
                "available": available,
                "period_start": period_start,
                "period_end": period_end,
                "budget_used_pct": used_pct
            })
        
        return overview
    
    @staticmethod
    def filter_budget_overview(
        overview: List[Dict[str, Any]],
        min_assigned: float = 0.0,
        strict: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Filter budget overview entries by assigned amount.
        
        Args:
            overview: List of budget overview dictionaries.
            min_assigned: Threshold for assigned amount filtering.
            strict: If True, require assigned > min_assigned; otherwise >=.
        
        Returns:
            Filtered list of overview entries.
        """
        if not overview:
            return []
        
        comparator = (lambda value: value > min_assigned) if strict else (lambda value: value >= min_assigned)
        filtered = [entry for entry in overview if comparator(float(entry.get("assigned", 0.0)))]
        
        logger.debug(
            "Filtered budget overview: %s -> %s entries (min_assigned=%s, strict=%s)",
            len(overview),
            len(filtered),
            min_assigned,
            strict
        )
        return filtered
    
    @staticmethod
    def calculate_budget_summary(overview: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Calculate aggregate budget summary metrics from overview entries.
        
        Args:
            overview: List of budget overview dictionaries.
        
        Returns:
            Dictionary with total_assigned, total_activity, total_available, budget_used_pct.
        """
        total_assigned = sum(float(entry.get("assigned", 0.0)) for entry in overview)
        total_activity = sum(float(entry.get("activity", 0.0)) for entry in overview)
        total_available = sum(float(entry.get("available", 0.0)) for entry in overview)
        budget_used_pct = (total_activity / total_assigned * 100.0) if total_assigned > 0 else 0.0
        
        summary = {
            "total_assigned": total_assigned,
            "total_activity": total_activity,
            "total_available": total_available,
            "budget_used_pct": budget_used_pct
        }
        
        logger.debug("Budget summary calculated: %s", summary)
        return summary
    
    def upsert_monthly_budget(self, category: str, month: date, allocated_amount: float) -> Optional[Budget]:
        """
        Create or update a monthly budget for the specified category.
        
        Args:
            category: Category name.
            month: Target month.
            allocated_amount: Assigned amount.
        
        Returns:
            Budget instance after update or creation, or None on failure.
        """
        period_start, period_end = self.get_month_period(month)
        existing = self.get_budget(category, period_start)
        if existing:
            logger.info(
                "Updating budget for category '%s' (%s - %s)",
                category,
                period_start,
                period_end
            )
            return self.update_budget(
                budget_id=existing.id,
                allocated_amount=allocated_amount,
                period_start=period_start,
                period_end=period_end
            )
        
        logger.info(
            "Creating new budget for category '%s' (%s - %s)",
            category,
            period_start,
            period_end
        )
        return self.create_budget(
            category=category,
            allocated_amount=allocated_amount,
            period_start=period_start,
            period_end=period_end
        )

