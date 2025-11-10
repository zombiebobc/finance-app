"""
Core data viewer module for financial transactions.

This module provides functions to query the database, apply filters,
and format data into pandas DataFrames for flexible display.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, date
import pandas as pd

from database_ops import DatabaseManager, Transaction

# Configure logging
logger = logging.getLogger(__name__)


class DataViewer:
    """
    Core viewer class for querying and formatting transaction data.
    
    This class provides a clean interface for retrieving and formatting
    transactions from the database, independent of the display method (CLI, UI, etc.).
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the data viewer.
        
        Args:
            db_manager: DatabaseManager instance for database access
        """
        self.db_manager = db_manager
        logger.info("Data viewer initialized")
    
    def get_transactions_df(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: str = "date",
        order_desc: bool = True
    ) -> pd.DataFrame:
        """
        Get transactions as a pandas DataFrame with optional filters.
        
        Args:
            filters: Dictionary of filter criteria (see database_ops.get_transactions)
            limit: Maximum number of records to return
            offset: Number of records to skip (for pagination)
            order_by: Column name to sort by (default: "date")
            order_desc: If True, sort descending; if False, ascending
        
        Returns:
            pandas DataFrame with columns: id, date, description, amount, category,
            account, source_file, import_timestamp
        
        Raises:
            ValueError: If filters are invalid
            SQLAlchemyError: If database query fails
        """
        # Validate filters
        if filters:
            self._validate_filters(filters)
        
        # Get transactions from database
        transactions = self.db_manager.get_transactions(
            filters=filters,
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_desc=order_desc
        )
        
        # Convert to DataFrame
        if not transactions:
            # Return empty DataFrame with correct columns
            return pd.DataFrame(columns=[
                "id", "date", "description", "amount", "category",
                "account", "source_file", "import_timestamp"
            ])
        
        # Build list of dictionaries
        data = []
        for trans in transactions:
            data.append({
                "id": trans.id,
                "date": trans.date,
                "description": trans.description,
                "amount": trans.amount,
                "category": trans.category,
                "account": trans.account,
                "source_file": trans.source_file,
                "import_timestamp": trans.import_timestamp
            })
        
        df = pd.DataFrame(data)
        
        logger.info(f"Retrieved {len(df)} transactions as DataFrame")
        return df
    
    def format_transactions_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Format transaction DataFrame for display.
        
        Formats dates to YYYY-MM-DD and amounts to currency format.
        Creates a copy to avoid modifying the original.
        
        Args:
            df: Input DataFrame with transaction data
        
        Returns:
            Formatted DataFrame with display-friendly formatting
        """
        if df.empty:
            return df
        
        # Create a copy to avoid modifying original
        formatted_df = df.copy()
        
        # Format dates
        if "date" in formatted_df.columns:
            formatted_df["date"] = pd.to_datetime(formatted_df["date"]).dt.strftime("%Y-%m-%d")
        
        if "import_timestamp" in formatted_df.columns:
            formatted_df["import_timestamp"] = pd.to_datetime(
                formatted_df["import_timestamp"]
            ).dt.strftime("%Y-%m-%d %H:%M:%S")
        
        # Format amounts (keep as float for calculations, but we'll format in display)
        # Amount formatting is typically done at display time, but we can add a formatted column
        if "amount" in formatted_df.columns:
            formatted_df["amount_formatted"] = formatted_df["amount"].apply(
                lambda x: f"${x:,.2f}"
            )
        
        logger.debug(f"Formatted {len(formatted_df)} transactions for display")
        return formatted_df
    
    def get_summary_stats(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get summary statistics for transactions matching filters.
        
        Args:
            filters: Optional filter criteria
        
        Returns:
            Dictionary with statistics:
                - total_count: Total number of transactions
                - total_amount: Sum of all amounts
                - average_amount: Average transaction amount
                - min_amount: Minimum transaction amount
                - max_amount: Maximum transaction amount
                - positive_count: Number of positive (credit) transactions
                - negative_count: Number of negative (debit) transactions
                - positive_total: Sum of positive amounts
                - negative_total: Sum of negative amounts
        """
        df = self.get_transactions_df(filters=filters)
        
        if df.empty:
            return {
                "total_count": 0,
                "total_amount": 0.0,
                "average_amount": 0.0,
                "min_amount": 0.0,
                "max_amount": 0.0,
                "positive_count": 0,
                "negative_count": 0,
                "positive_total": 0.0,
                "negative_total": 0.0
            }
        
        amounts = df["amount"]
        
        stats = {
            "total_count": len(df),
            "total_amount": float(amounts.sum()),
            "average_amount": float(amounts.mean()),
            "min_amount": float(amounts.min()),
            "max_amount": float(amounts.max()),
            "positive_count": int((amounts > 0).sum()),
            "negative_count": int((amounts < 0).sum()),
            "positive_total": float(amounts[amounts > 0].sum()),
            "negative_total": float(amounts[amounts < 0].sum())
        }
        
        logger.debug(f"Calculated summary stats: {stats}")
        return stats
    
    def _validate_filters(self, filters: Dict[str, Any]) -> None:
        """
        Validate filter parameters.
        
        Args:
            filters: Dictionary of filter criteria
        
        Raises:
            ValueError: If any filter is invalid
        """
        # Validate account_id filter
        if "account_id" in filters and filters["account_id"] is not None:
            try:
                int(filters["account_id"])
            except (ValueError, TypeError):
                raise ValueError("account_id must be an integer")
        
        # Validate date filters
        for date_key in ["date_start", "date_end"]:
            if date_key in filters and filters[date_key]:
                try:
                    date_val = filters[date_key]
                    if isinstance(date_val, str):
                        datetime.fromisoformat(date_val.replace("Z", "+00:00"))
                    elif not isinstance(date_val, (date, datetime)):
                        raise ValueError(f"{date_key} must be a date, datetime, or ISO string")
                except (ValueError, AttributeError) as e:
                    raise ValueError(f"Invalid {date_key} format: {e}")
        
        # Validate amount filters
        for amount_key in ["amount_min", "amount_max"]:
            if amount_key in filters and filters[amount_key] is not None:
                try:
                    float(filters[amount_key])
                except (ValueError, TypeError):
                    raise ValueError(f"{amount_key} must be a number")
        
        # Validate date range
        if "date_start" in filters and "date_end" in filters:
            if filters["date_start"] and filters["date_end"]:
                date_start = filters["date_start"]
                date_end = filters["date_end"]
                
                # Convert to datetime for comparison
                if isinstance(date_start, str):
                    date_start = datetime.fromisoformat(date_start.replace("Z", "+00:00"))
                elif isinstance(date_start, date) and not isinstance(date_start, datetime):
                    date_start = datetime.combine(date_start, datetime.min.time())
                
                if isinstance(date_end, str):
                    date_end = datetime.fromisoformat(date_end.replace("Z", "+00:00"))
                elif isinstance(date_end, date) and not isinstance(date_end, datetime):
                    date_end = datetime.combine(date_end, datetime.max.time())
                
                if date_start > date_end:
                    raise ValueError("date_start must be before or equal to date_end")
        
        # Validate amount range
        if "amount_min" in filters and "amount_max" in filters:
            if filters["amount_min"] is not None and filters["amount_max"] is not None:
                if float(filters["amount_min"]) > float(filters["amount_max"]):
                    raise ValueError("amount_min must be less than or equal to amount_max")
        
        logger.debug("Filter validation passed")
    
    def export_to_csv(self, df: pd.DataFrame, file_path: str) -> None:
        """
        Export transaction DataFrame to CSV file.
        
        Args:
            df: DataFrame to export
            file_path: Path to output CSV file
        
        Raises:
            IOError: If file cannot be written
        """
        try:
            df.to_csv(file_path, index=False)
            logger.info(f"Exported {len(df)} transactions to {file_path}")
        except Exception as e:
            logger.error(f"Failed to export to CSV: {e}")
            raise IOError(f"Failed to export to CSV: {e}") from e

