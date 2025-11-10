"""
Budgeting module for YNAB-like budget management.

This module provides budget envelope functionality, allowing users to
allocate funds to categories and track spending against budgets.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from dataclasses import dataclass

from database_ops import DatabaseManager, Budget, Transaction
from sqlalchemy.exc import SQLAlchemyError

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
            # Check for overlapping budgets
            existing = session.query(Budget).filter(
                Budget.category == category,
                Budget.period_start <= period_end,
                Budget.period_end >= period_start
            ).first()
            
            if existing:
                logger.warning(
                    f"Budget already exists for category '{category}' "
                    f"overlapping period {period_start} to {period_end}"
                )
                # Update existing instead
                existing.allocated_amount = allocated_amount
                existing.period_start = datetime.combine(period_start, datetime.min.time())
                existing.period_end = datetime.combine(period_end, datetime.max.time())
                existing.updated_at = datetime.utcnow()
                session.commit()
                return existing
            
            budget = Budget(
                category=category,
                allocated_amount=allocated_amount,
                period_start=datetime.combine(period_start, datetime.min.time()),
                period_end=datetime.combine(period_end, datetime.max.time())
            )
            
            session.add(budget)
            session.commit()
            
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
            
            budget = session.query(Budget).filter(
                Budget.category == category,
                Budget.period_start <= period_datetime,
                Budget.period_end >= period_datetime
            ).first()
            
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
            start_datetime = datetime.combine(period_start, datetime.min.time())
            end_datetime = datetime.combine(period_end, datetime.max.time())
            
            # Sum negative amounts (spending) for this category
            result = session.query(Transaction.amount).filter(
                Transaction.category == category,
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
            from sqlalchemy import func, distinct
            
            # Get all unique categories from transactions
            categories = session.query(
                func.coalesce(Transaction.category, 'Uncategorized')
            ).distinct().all()
            
            # Flatten and sort
            category_list = sorted([cat[0] for cat in categories if cat[0]])
            
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

