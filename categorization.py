"""
Categorization module for automatic transaction categorization.

This module provides a rules-based engine for automatically assigning
categories to transactions based on description patterns, amounts, etc.
"""

import logging
import re
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class CategorizationRule:
    """
    A rule for categorizing transactions.
    
    Attributes:
        pattern: Regex pattern or string to match in description
        category: Category to assign if pattern matches
        priority: Rule priority (higher = checked first)
        case_sensitive: Whether pattern matching is case-sensitive
        amount_min: Optional minimum amount filter
        amount_max: Optional maximum amount filter
    """
    pattern: str
    category: str
    priority: int = 0
    case_sensitive: bool = False
    amount_min: Optional[float] = None
    amount_max: Optional[float] = None
    
    def matches(self, description: str, amount: Optional[float] = None) -> bool:
        """
        Check if this rule matches a transaction.
        
        Args:
            description: Transaction description
            amount: Optional transaction amount
        
        Returns:
            True if rule matches, False otherwise
        """
        # Check amount filters first
        if amount is not None:
            if self.amount_min is not None and amount < self.amount_min:
                return False
            if self.amount_max is not None and amount > self.amount_max:
                return False
        
        # Check description pattern
        flags = 0 if self.case_sensitive else re.IGNORECASE
        
        try:
            if re.search(self.pattern, description, flags):
                return True
        except re.error as e:
            logger.warning(f"Invalid regex pattern '{self.pattern}': {e}")
            # Fallback to simple substring match
            if self.case_sensitive:
                return self.pattern in description
            else:
                return self.pattern.lower() in description.lower()
        
        return False


class CategorizationEngine:
    """
    Rules-based engine for categorizing transactions.
    
    Uses a set of rules (regex patterns, amount filters, etc.) to
    automatically assign categories to transactions.
    """
    
    def __init__(self, rules: Optional[List[CategorizationRule]] = None):
        """
        Initialize the categorization engine.
        
        Args:
            rules: Optional list of initial rules
        """
        self.rules: List[CategorizationRule] = rules or []
        # Sort rules by priority (higher first)
        self.rules.sort(key=lambda r: r.priority, reverse=True)
        logger.info(f"Categorization engine initialized with {len(self.rules)} rules")
    
    def add_rule(
        self,
        pattern: str,
        category: str,
        priority: int = 0,
        case_sensitive: bool = False,
        amount_min: Optional[float] = None,
        amount_max: Optional[float] = None
    ) -> None:
        """
        Add a categorization rule.
        
        Args:
            pattern: Regex pattern or string to match
            category: Category to assign
            priority: Rule priority (higher = checked first)
            case_sensitive: Whether matching is case-sensitive
            amount_min: Optional minimum amount filter
            amount_max: Optional maximum amount filter
        """
        rule = CategorizationRule(
            pattern=pattern,
            category=category,
            priority=priority,
            case_sensitive=case_sensitive,
            amount_min=amount_min,
            amount_max=amount_max
        )
        self.rules.append(rule)
        # Re-sort by priority
        self.rules.sort(key=lambda r: r.priority, reverse=True)
        logger.debug(f"Added categorization rule: {pattern} -> {category}")
    
    def categorize(
        self,
        description: str,
        amount: Optional[float] = None,
        existing_category: Optional[str] = None
    ) -> Optional[str]:
        """
        Categorize a transaction based on its description and amount.
        
        Args:
            description: Transaction description
            amount: Optional transaction amount
            existing_category: Optional existing category (if already set, may skip)
        
        Returns:
            Category name if a rule matches, None otherwise
        """
        # If category already exists and is not empty, don't override
        if existing_category:
            return existing_category
        
        # Check each rule in priority order
        for rule in self.rules:
            if rule.matches(description, amount):
                logger.debug(f"Matched rule '{rule.pattern}' -> '{rule.category}' for '{description}'")
                return rule.category
        
        return None
    
    def get_default_rules(self) -> List[CategorizationRule]:
        """
        Get a set of default categorization rules.
        
        Returns:
            List of default CategorizationRule objects
        """
        default_rules = [
            # Food & Dining
            CategorizationRule(r"AMAZON|WALMART|TARGET|COSTCO", "Shopping", priority=10),
            CategorizationRule(r"STARBUCKS|DUNKIN|COFFEE", "Food & Drink", priority=10),
            CategorizationRule(r"RESTAURANT|CAFE|PIZZA|BURGER|TACO|CHIPOTLE|MCDONALD", "Food & Drink", priority=10),
            CategorizationRule(r"WENDY|ARBY|KFC|SUBWAY", "Food & Drink", priority=9),
            CategorizationRule(r"GROCERY|SUPERMARKET|FOOD|MARKET", "Groceries", priority=10),
            
            # Transportation
            CategorizationRule(r"GAS|FUEL|SHELL|EXXON|BP|CHEVRON", "Transportation", priority=10),
            CategorizationRule(r"UBER|LYFT|TAXI", "Transportation", priority=10),
            CategorizationRule(r"PARKING|TOLL", "Transportation", priority=8),
            
            # Entertainment
            CategorizationRule(r"STEAM|NETFLIX|SPOTIFY|HULU|DISNEY", "Entertainment", priority=10),
            CategorizationRule(r"MOVIE|CINEMA|THEATER", "Entertainment", priority=9),
            CategorizationRule(r"PATREON|YOUTUBE", "Entertainment", priority=8),
            
            # Bills & Utilities
            CategorizationRule(r"ELECTRIC|POWER|UTILITY|WATER|SEWER", "Bills & Utilities", priority=10),
            CategorizationRule(r"INTERNET|PHONE|CELLULAR|VERIZON|AT&T", "Bills & Utilities", priority=10),
            CategorizationRule(r"INSURANCE|PREMIUM", "Bills & Utilities", priority=9),
            
            # Income (ONLY for positive amounts - deposits)
            CategorizationRule(r"PAYCHECK|PAYROLL|DIRECT DEP", "Paycheck", priority=15, amount_min=0.01),
            CategorizationRule(r"BRIGHTSTAR.*PAYROLL|BRIGHTSTAR.*DIRECT DEP", "Paycheck", priority=16, amount_min=0.01),
            CategorizationRule(r"SALARY|WAGES|PAY", "Paycheck", priority=14, amount_min=100),
            CategorizationRule(r"REFUND|RETURN|REIMBURSEMENT", "Refund", priority=12, amount_min=0.01),
            CategorizationRule(r"DEPOSIT|CREDIT", "Income", priority=8, amount_min=50),
            
            # Cash Withdrawals
            CategorizationRule(r"ATM.*CASH WITHDRAWAL|CASH WITHDRAWAL", "Cash Withdrawal", priority=15, amount_max=-0.01),
            CategorizationRule(r"ATM|WITHDRAWAL", "Cash Withdrawal", priority=10, amount_max=-0.01),
            
            # Transfers
            CategorizationRule(r"TRANSFER|MOVE MONEY", "Transfer", priority=5),
            CategorizationRule(r"ZELLE", "Transfer", priority=6),  # Zelle can be income or expense
            
            # Shopping
            CategorizationRule(r"AMAZON|EBAY|ETSY", "Shopping", priority=10),
            CategorizationRule(r"APPLE|GOOGLE|MICROSOFT", "Shopping", priority=9),
        ]
        
        return default_rules
    
    def load_default_rules(self) -> None:
        """Load default categorization rules."""
        self.rules = self.get_default_rules()
        self.rules.sort(key=lambda r: r.priority, reverse=True)
        logger.info(f"Loaded {len(self.rules)} default categorization rules")
    
    def load_rules_from_dict(self, rules_data: List[Dict[str, Any]]) -> None:
        """
        Load rules from a dictionary/list format.
        
        Args:
            rules_data: List of rule dictionaries with keys:
                - pattern: Regex pattern
                - category: Category name
                - priority: Optional priority (default: 0)
                - case_sensitive: Optional case sensitivity (default: False)
                - amount_min: Optional minimum amount
                - amount_max: Optional maximum amount
        """
        self.rules = []
        for rule_data in rules_data:
            rule = CategorizationRule(
                pattern=rule_data.get("pattern", ""),
                category=rule_data.get("category", ""),
                priority=rule_data.get("priority", 0),
                case_sensitive=rule_data.get("case_sensitive", False),
                amount_min=rule_data.get("amount_min"),
                amount_max=rule_data.get("amount_max")
            )
            self.rules.append(rule)
        
        self.rules.sort(key=lambda r: r.priority, reverse=True)
        logger.info(f"Loaded {len(self.rules)} rules from dictionary")
    
    def export_rules_to_dict(self) -> List[Dict[str, Any]]:
        """
        Export rules to dictionary format.
        
        Returns:
            List of rule dictionaries
        """
        return [
            {
                "pattern": rule.pattern,
                "category": rule.category,
                "priority": rule.priority,
                "case_sensitive": rule.case_sensitive,
                "amount_min": rule.amount_min,
                "amount_max": rule.amount_max
            }
            for rule in self.rules
        ]

