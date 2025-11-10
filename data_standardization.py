"""
Data standardization module for financial transactions.

This module maps CSV columns to standard field names using fuzzy matching,
converts data types, and standardizes formats (dates, amounts, etc.).
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
from difflib import SequenceMatcher

# Configure logging
logger = logging.getLogger(__name__)


class DataStandardizer:
    """
    Standardizes financial transaction data from various CSV formats.
    
    This class handles:
    - Fuzzy column name matching
    - Data type conversion
    - Date format standardization
    - Amount normalization
    """
    
    def __init__(
        self,
        column_mappings: Dict[str, List[str]],
        date_formats: List[str],
        output_date_format: str = "%Y-%m-%d",
        amount_decimal_places: int = 2
    ):
        """
        Initialize the data standardizer.
        
        Args:
            column_mappings: Dictionary mapping standard field names to lists of
                possible column name variations (e.g., {'date': ['date', 'transaction date']})
            date_formats: List of date format strings to try when parsing dates
            output_date_format: Format string for output dates (default: ISO format)
            amount_decimal_places: Number of decimal places for amounts (default: 2)
        """
        self.column_mappings = column_mappings
        self.date_formats = date_formats
        self.output_date_format = output_date_format
        self.amount_decimal_places = amount_decimal_places
        
        logger.info("Data standardizer initialized")
    
    def _fuzzy_match_column(
        self,
        column_name: str,
        target_variations: List[str],
        threshold: float = 0.6
    ) -> bool:
        """
        Check if a column name fuzzy matches any of the target variations.
        
        Args:
            column_name: The column name to check
            target_variations: List of target column name variations
            threshold: Similarity threshold (0.0 to 1.0, default: 0.6)
        
        Returns:
            True if similarity exceeds threshold, False otherwise
        """
        column_lower = column_name.lower().strip()
        
        for variation in target_variations:
            variation_lower = variation.lower().strip()
            
            # Exact match
            if column_lower == variation_lower:
                return True
            
            # Substring match (column contains variation or vice versa)
            if variation_lower in column_lower or column_lower in variation_lower:
                return True
            
            # Fuzzy similarity match
            similarity = SequenceMatcher(None, column_lower, variation_lower).ratio()
            if similarity >= threshold:
                return True
        
        return False
    
    def map_columns(self, csv_columns: List[str]) -> Dict[str, Optional[str]]:
        """
        Map CSV column names to standard field names using fuzzy matching.
        
        Args:
            csv_columns: List of column names from the CSV file
        
        Returns:
            Dictionary mapping standard field names to CSV column names
            (None if no match found for a standard field)
        """
        mapping = {}
        used_columns = set()  # Track columns that have been matched
        
        # Sort standard fields by priority (date, description, amount are most important)
        priority_order = ["date", "description", "amount", "category", "account"]
        standard_fields = sorted(
            self.column_mappings.keys(),
            key=lambda x: priority_order.index(x) if x in priority_order else 999
        )
        
        for standard_field in standard_fields:
            variations = self.column_mappings[standard_field]
            matched_column = None
            best_match_score = 0
            
            # Try to find the best match (exact > substring > fuzzy)
            for col in csv_columns:
                if col in used_columns:
                    continue  # Skip already matched columns
                
                col_lower = col.lower().strip()
                
                # Check for exact match
                for variation in variations:
                    if col_lower == variation.lower().strip():
                        matched_column = col
                        best_match_score = 3  # Highest priority
                        break
                
                if matched_column:
                    break
                
                # Check for substring match (more specific)
                for variation in variations:
                    var_lower = variation.lower().strip()
                    # Check if variation is a significant part of the column name
                    if var_lower in col_lower and len(var_lower) >= 4:  # At least 4 chars for meaningful match
                        # Prefer matches where the variation is a significant portion
                        if len(var_lower) / len(col_lower) > 0.5 or col_lower.startswith(var_lower) or col_lower.endswith(var_lower):
                            score = 2
                            if score > best_match_score:
                                matched_column = col
                                best_match_score = score
                
                # Check fuzzy similarity as last resort
                if not matched_column or best_match_score < 2:
                    for variation in variations:
                        similarity = SequenceMatcher(None, col_lower, variation.lower().strip()).ratio()
                        if similarity >= 0.7 and similarity > best_match_score / 3:  # Higher threshold
                            matched_column = col
                            best_match_score = max(best_match_score, similarity * 3)
            
            if matched_column:
                used_columns.add(matched_column)
                mapping[standard_field] = matched_column
                logger.debug(f"Mapped '{matched_column}' -> '{standard_field}'")
            else:
                mapping[standard_field] = None
                logger.warning(f"No match found for standard field '{standard_field}'")
        
        return mapping
    
    def _parse_date(self, date_value: Any) -> Optional[datetime]:
        """
        Parse a date value using multiple format attempts.
        
        Args:
            date_value: Date value (string, datetime, or other)
        
        Returns:
            Parsed datetime object, or None if parsing fails
        """
        if pd.isna(date_value) or date_value is None:
            return None
        
        # If already a datetime, return it
        if isinstance(date_value, datetime):
            return date_value
        
        # If it's a pandas Timestamp, convert to datetime
        if isinstance(date_value, pd.Timestamp):
            return date_value.to_pydatetime()
        
        # Try to parse as string
        date_str = str(date_value).strip()
        if not date_str:
            return None
        
        # Try each date format
        for date_format in self.date_formats:
            try:
                parsed_date = datetime.strptime(date_str, date_format)
                return parsed_date
            except (ValueError, TypeError):
                continue
        
        # Try pandas parsing as fallback
        try:
            parsed_date = pd.to_datetime(date_str)
            if isinstance(parsed_date, pd.Timestamp):
                return parsed_date.to_pydatetime()
            return parsed_date
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse date: {date_value}")
            return None
    
    def _parse_amount(self, amount_value: Any) -> Optional[float]:
        """
        Parse and normalize an amount value.
        
        Args:
            amount_value: Amount value (string, number, or other)
        
        Returns:
            Normalized float value rounded to specified decimal places, or None if parsing fails
        """
        if pd.isna(amount_value) or amount_value is None:
            return None
        
        # If already a number, convert and round
        if isinstance(amount_value, (int, float)):
            return round(float(amount_value), self.amount_decimal_places)
        
        # Try to parse as string
        amount_str = str(amount_value).strip()
        if not amount_str:
            return None
        
        # Remove common currency symbols and formatting
        amount_str = amount_str.replace("$", "").replace(",", "").replace(" ", "")
        
        try:
            amount_float = float(amount_str)
            return round(amount_float, self.amount_decimal_places)
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse amount: {amount_value}")
            return None
    
    def _parse_string(self, value: Any, max_length: Optional[int] = None) -> Optional[str]:
        """
        Parse a string value, optionally truncating to max_length.
        
        Args:
            value: Value to convert to string
            max_length: Optional maximum length (truncates if longer)
        
        Returns:
            String value, or None if conversion fails
        """
        if pd.isna(value) or value is None:
            return None
        
        str_value = str(value).strip()
        
        if max_length and len(str_value) > max_length:
            str_value = str_value[:max_length]
            logger.debug(f"Truncated string to {max_length} characters")
        
        return str_value if str_value else None
    
    def standardize_row(
        self,
        row: pd.Series,
        column_mapping: Dict[str, Optional[str]]
    ) -> Dict[str, Any]:
        """
        Standardize a single row of transaction data.
        
        Args:
            row: Pandas Series representing one row of data
            column_mapping: Dictionary mapping standard fields to CSV column names
        
        Returns:
            Dictionary with standardized transaction data
        """
        standardized = {}
        
        # Map and parse each standard field
        for standard_field, csv_column in column_mapping.items():
            if csv_column is None or csv_column not in row.index:
                standardized[standard_field] = None
                continue
            
            value = row[csv_column]
            
            # Parse based on field type
            if standard_field == "date":
                standardized[standard_field] = self._parse_date(value)
            elif standard_field == "amount":
                standardized[standard_field] = self._parse_amount(value)
            elif standard_field == "description":
                standardized[standard_field] = self._parse_string(value, max_length=500)
            elif standard_field == "category":
                standardized[standard_field] = self._parse_string(value, max_length=100)
            elif standard_field == "account":
                standardized[standard_field] = self._parse_string(value, max_length=100)
            else:
                # Default: convert to string
                standardized[standard_field] = self._parse_string(value)
        
        return standardized
    
    def standardize_dataframe(
        self,
        df: pd.DataFrame,
        source_file: str
    ) -> List[Dict[str, Any]]:
        """
        Standardize an entire DataFrame of transaction data.
        
        Args:
            df: Pandas DataFrame containing transaction data
            source_file: Name/path of the source CSV file
        
        Returns:
            List of standardized transaction dictionaries
        
        Raises:
            ValueError: If required fields (date, description, amount) are missing
        """
        # Map columns
        column_mapping = self.map_columns(df.columns.tolist())
        
        # Check for required fields
        required_fields = ["date", "description", "amount"]
        missing_fields = [
            field for field in required_fields
            if column_mapping.get(field) is None
        ]
        
        if missing_fields:
            raise ValueError(
                f"Missing required fields in CSV: {missing_fields}. "
                f"Available columns: {df.columns.tolist()}"
            )
        
        standardized_transactions = []
        skipped_rows = 0
        
        # Process each row
        for idx, row in df.iterrows():
            try:
                standardized = self.standardize_row(row, column_mapping)
                
                # Validate required fields
                if (
                    standardized.get("date") is None or
                    standardized.get("description") is None or
                    standardized.get("amount") is None
                ):
                    logger.warning(
                        f"Row {idx} skipped: missing required fields "
                        f"(date={standardized.get('date')}, "
                        f"description={standardized.get('description')}, "
                        f"amount={standardized.get('amount')})"
                    )
                    skipped_rows += 1
                    continue
                
                # Add source file information
                standardized["source_file"] = source_file
                
                standardized_transactions.append(standardized)
                
            except Exception as e:
                logger.warning(f"Error standardizing row {idx}: {e}")
                skipped_rows += 1
                continue
        
        logger.info(
            f"Standardized {len(standardized_transactions)} transactions, "
            f"skipped {skipped_rows} rows"
        )
        
        return standardized_transactions

