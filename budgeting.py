"""
Budgeting module for YNAB-like budget management.

This module provides budget envelope functionality, allowing users to
allocate funds to categories and track spending against budgets.
"""

import logging
from typing import List, Optional, Dict, Any, Tuple, Set
from datetime import datetime, date, timedelta
from dataclasses import dataclass

from database_ops import DatabaseManager, Budget, Transaction
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

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
        
        return sorted(unique.values(), key=str.casefold)
    
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
                existing.updated_at = datetime.utcnow()
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
            
            budget = session.query(Budget).filter(
                func.lower(Budget.category) == category_key,
                Budget.period_start <= period_datetime,
                Budget.period_end >= period_datetime
            ).first()
            
            if budget:
                session.refresh(budget)
                session.expunge(budget)
            return budget
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
        
        Args:
            category: Category name
            period_start: Start date (defaults to start of current month)
            period_end: End date (defaults to end of current month)
        
        Returns:
            Total spending amount
        """
        if period_start is None:
            period_start = date.today().replace(day=1)
        if period_end is None:
            # Last day of current month
            next_month = period_start.replace(day=28) + timedelta(days=4)
            period_end = next_month - timedelta(days=next_month.day)
        
        session = self.db_manager.get_session()
        try:
            normalized_category = self._normalize_category(category)
            if not normalized_category:
                return 0.0
            category_key = self._category_key(normalized_category)
            
            start_datetime = datetime.combine(period_start, datetime.min.time())
            end_datetime = datetime.combine(period_end, datetime.max.time())
            
            # Sum negative amounts (spending) for this category
            result = session.query(Transaction.amount).filter(
                Transaction.category.isnot(None),
                func.lower(Transaction.category) == category_key,
                Transaction.date >= start_datetime,
                Transaction.date <= end_datetime,
                Transaction.is_transfer == 0,  # Exclude transfers
                Transaction.amount < 0  # Only spending (negative amounts)
            ).all()
            
            total = sum(abs(amount[0]) for amount in result) if result else 0.0
            return total
        except SQLAlchemyError as e:
            logger.error(f"Failed to calculate category spending: {e}")
            return 0.0
        finally:
            session.close()
    
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
            
            budget.updated_at = datetime.utcnow()
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
            raw_categories = session.query(
                func.trim(Transaction.category)
            ).filter(
                Transaction.category.isnot(None),
                func.trim(Transaction.category) != "",
                Transaction.is_transfer == 0
            ).distinct().all()
            
            unique: Dict[str, str] = {}
            for (category,) in raw_categories:
                normalized = self._normalize_category(category)
                if not normalized:
                    continue
                key = self._category_key(normalized)
                if key == "transfer":
                    continue
                if key not in unique:
                    unique[key] = normalized
            
            category_list = sorted(unique.values(), key=str.casefold)
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
        all_categories = categories if categories is not None else self.get_budget_categories()
        if not all_categories:
            return []
        
        existing_budgets = self.get_monthly_budgets(month)
        existing_keys: Set[str] = {self._category_key(budget.category) for budget in existing_budgets}
        
        available = [
            category for category in all_categories
            if self._category_key(category) not in existing_keys
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
        
        for budget in budgets:
            period_start = budget.period_start.date()
            period_end = budget.period_end.date()
            activity = self.calculate_category_spending(
                budget.category,
                period_start=period_start,
                period_end=period_end
            )
            available = budget.allocated_amount - activity
            used_pct = (activity / budget.allocated_amount * 100) if budget.allocated_amount > 0 else 0.0
            
            overview.append({
                "id": budget.id,
                "category": budget.category,
                "assigned": budget.allocated_amount,
                "activity": activity,
                "available": available,
                "period_start": period_start,
                "period_end": period_end,
                "budget_used_pct": used_pct
            })
        
        return overview
    
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

