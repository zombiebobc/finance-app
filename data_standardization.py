"""
Data standardization module for financial transactions.

This module maps CSV columns to standard field names using fuzzy matching,
converts data types, and standardizes formats (dates, amounts, etc.).
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd
from difflib import SequenceMatcher

# Added: Custom exception and prompt helper for resilient standardization
from utils import StandardizationError, prompt_user_choice

# Configure logging
logger = logging.getLogger(__name__)

PromptHandler = Callable[[str, Dict[str, str], str], str]


class DataStandardizer:
    """
    Standardizes financial transaction data from various CSV formats.

    This class handles:
    - Fuzzy column name matching
    - Data type conversion
    - Date format standardization
    - Amount normalization
    - Interactive remediation for malformed records
    """

    REQUIRED_FIELDS = ("date", "description", "amount")

    def __init__(
        self,
        column_mappings: Dict[str, List[str]],
        date_formats: List[str],
        output_date_format: str = "%Y-%m-%d",
        amount_decimal_places: int = 2,
        *,
        max_error_rows: Optional[int] = None,
        max_error_ratio: float = 0.1,
        fallback_values: Optional[Dict[str, Any]] = None,
        prompt_handler: Optional[PromptHandler] = None,
    ):
        """
        Initialize the data standardizer.

        Args:
            column_mappings: Mapping of standard field names to lists of possible CSV column variations.
            date_formats: List of date format strings to try when parsing dates.
            output_date_format: Format string for output dates (default: ISO format).
            amount_decimal_places: Number of decimal places for amounts (default: 2).
            max_error_rows: Maximum number of row-level errors to tolerate before aborting.
            max_error_ratio: Maximum ratio of errors allowed relative to total rows.
            fallback_values: Optional defaults for missing required fields (e.g., {"amount": 0.0}).
            prompt_handler: Optional callable for interactive remediation prompts.
        """
        self.column_mappings = column_mappings
        self.date_formats = date_formats
        self.output_date_format = output_date_format
        self.amount_decimal_places = amount_decimal_places
        self.max_error_rows = max_error_rows
        self.max_error_ratio = max_error_ratio
        self.fallback_values = fallback_values.copy() if fallback_values else {}
        self.prompt_handler = prompt_handler or prompt_user_choice

        logger.info(
            "Data standardizer initialized (decimal_places=%s, max_error_rows=%s, max_error_ratio=%s)",
            amount_decimal_places,
            max_error_rows,
            max_error_ratio,
        )

    def _fuzzy_match_column(
        self,
        column_name: str,
        target_variations: List[str],
        threshold: float = 0.6,
    ) -> bool:
        """
        Check if a column name fuzzy matches any of the target variations.

        Args:
            column_name: The column name to check.
            target_variations: List of target column name variations.
            threshold: Similarity threshold (0.0 to 1.0, default: 0.6).

        Returns:
            True if similarity exceeds threshold, False otherwise.
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
            csv_columns: List of column names from the CSV file.

        Returns:
            Dictionary mapping standard field names to CSV column names (None if no match found).
        """
        mapping: Dict[str, Optional[str]] = {}
        used_columns = set()

        priority_order = ["date", "description", "amount", "category", "account"]
        standard_fields = sorted(
            self.column_mappings.keys(),
            key=lambda field: priority_order.index(field) if field in priority_order else 999,
        )

        for standard_field in standard_fields:
            variations = self.column_mappings[standard_field]
            matched_column = None
            best_match_score = 0

            for col in csv_columns:
                if col in used_columns:
                    continue

                col_lower = col.lower().strip()

                # Exact match
                for variation in variations:
                    if col_lower == variation.lower().strip():
                        matched_column = col
                        best_match_score = 3
                        break

                if matched_column:
                    break

                # Substring match
                for variation in variations:
                    var_lower = variation.lower().strip()
                    if var_lower in col_lower and len(var_lower) >= 4:
                        if len(var_lower) / len(col_lower) > 0.5 or col_lower.startswith(var_lower) or col_lower.endswith(var_lower):
                            score = 2
                            if score > best_match_score:
                                matched_column = col
                                best_match_score = score

                # Fuzzy similarity
                if not matched_column or best_match_score < 2:
                    for variation in variations:
                        similarity = SequenceMatcher(None, col_lower, variation.lower().strip()).ratio()
                        if similarity >= 0.7 and similarity > best_match_score / 3:
                            matched_column = col
                            best_match_score = max(best_match_score, similarity * 3)

            if matched_column:
                used_columns.add(matched_column)
                mapping[standard_field] = matched_column
                logger.debug("Mapped '%s' -> '%s'", matched_column, standard_field)
            else:
                mapping[standard_field] = None
                logger.warning("No match found for standard field '%s'", standard_field)

        return mapping

    def _parse_date(self, date_value: Any) -> Optional[datetime]:
        """
        Parse a date value using multiple format attempts.

        Args:
            date_value: Date value (string, datetime, or other).

        Returns:
            Parsed datetime object, or None if parsing fails.
        """
        if pd.isna(date_value) or date_value is None:
            return None

        if isinstance(date_value, datetime):
            return date_value

        if isinstance(date_value, pd.Timestamp):
            return date_value.to_pydatetime()

        date_str = str(date_value).strip()
        if not date_str:
            return None

        for date_format in self.date_formats:
            try:
                parsed_date = datetime.strptime(date_str, date_format)
                return parsed_date
            except (ValueError, TypeError):
                continue

        try:
            parsed_date = pd.to_datetime(date_str)
            if isinstance(parsed_date, pd.Timestamp):
                return parsed_date.to_pydatetime()
            return parsed_date
        except (ValueError, TypeError):
            logger.warning("Failed to parse date value '%s'", date_value)
            return None

    def _parse_amount(self, amount_value: Any) -> Optional[float]:
        """
        Parse and normalize an amount value.

        Args:
            amount_value: Amount value (string, number, or other).

        Returns:
            Normalized float value rounded to specified decimal places, or None if parsing fails.
        """
        if pd.isna(amount_value) or amount_value is None:
            return None

        if isinstance(amount_value, (int, float)):
            return round(float(amount_value), self.amount_decimal_places)

        amount_str = str(amount_value).strip()
        if not amount_str:
            return None

        amount_str = amount_str.replace("$", "").replace(",", "").replace(" ", "")

        try:
            amount_float = float(amount_str)
            return round(amount_float, self.amount_decimal_places)
        except (ValueError, TypeError):
            logger.warning("Failed to parse amount value '%s'", amount_value)
            return None

    def _parse_string(self, value: Any, max_length: Optional[int] = None) -> Optional[str]:
        """
        Parse a string value, optionally truncating to max_length.

        Args:
            value: Value to convert to string.
            max_length: Optional maximum length (truncates if longer).

        Returns:
            String value, or None if conversion fails.
        """
        if pd.isna(value) or value is None:
            return None

        str_value = str(value).strip()

        if max_length and len(str_value) > max_length:
            str_value = str_value[:max_length]
            logger.debug("Truncated string to %s characters", max_length)

        return str_value if str_value else None

    def _compute_error_budget(self, total_rows: int) -> Optional[int]:
        """Determine how many row-level failures are allowed before aborting."""
        if total_rows <= 0:
            return 0 if self.max_error_rows is not None else None

        budgets = []

        if self.max_error_ratio > 0:
            ratio_budget = max(1, math.ceil(total_rows * self.max_error_ratio))
            budgets.append(ratio_budget)

        if self.max_error_rows is not None:
            budgets.append(self.max_error_rows)

        if not budgets:
            return None

        return min(budgets)

    def _resolve_required_field(
        self,
        field_name: str,
        row_index: int,
        source_file: str,
    ) -> Optional[Any]:
        """
        Resolve a missing required field via defaults or prompts.
        """
        issue = f"Row {row_index} in '{source_file}' missing required field '{field_name}'."
        options = {"s": "Skip row", "d": "Use configured default", "a": "Abort"}
        selection = self.prompt_handler(issue, options, default="s")

        if selection == "d":
            fallback = self.fallback_values.get(field_name)
            if callable(fallback):
                fallback = fallback()
            if fallback is not None:
                logger.info("Applying fallback for field '%s' on row %s using value %s", field_name, row_index, fallback)
                return fallback
            logger.warning("No fallback configured for field '%s'; skipping row.", field_name)
            return None

        if selection == "a":
            raise StandardizationError(issue)

        logger.debug("Skipping row %s due to missing '%s'.", row_index, field_name)
        return None

    def _ensure_required_fields(
        self,
        standardized: Dict[str, Any],
        row_index: int,
        source_file: str,
    ) -> bool:
        """
        Ensure required fields are populated, applying fallbacks or prompts if needed.
        """
        missing_fields = [
            field_name for field_name in self.REQUIRED_FIELDS if standardized.get(field_name) in {None, ""}
        ]

        if not missing_fields:
            return True

        for field_name in missing_fields:
            fallback_value = self._resolve_required_field(field_name, row_index, source_file)
            if fallback_value is not None:
                standardized[field_name] = fallback_value
            else:
                return False

        still_missing = [
            field_name for field_name in self.REQUIRED_FIELDS if standardized.get(field_name) in {None, ""}
        ]
        return not still_missing

    def standardize_row(
        self,
        row: pd.Series,
        column_mapping: Dict[str, Optional[str]],
        *,
        row_index: Optional[int] = None,
        source_file: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Standardize a single row of transaction data.

        Args:
            row: Pandas Series representing one row of data.
            column_mapping: Mapping of standard fields to CSV column names.
            row_index: Optional index for logging/debugging.
            source_file: Optional source file name for context.

        Returns:
            Dictionary with standardized transaction data.
        """
        standardized: Dict[str, Any] = {}

        for standard_field, csv_column in column_mapping.items():
            if csv_column is None or csv_column not in row.index:
                standardized[standard_field] = None
                continue

            value = row[csv_column]

            if standard_field == "date":
                parsed_date = self._parse_date(value)
                if parsed_date is None and row_index is not None:
                    logger.debug(
                        "Row %s (%s) has unparsable date value '%s' (column '%s').",
                        row_index,
                        source_file,
                        value,
                        csv_column,
                    )
                standardized[standard_field] = parsed_date
            elif standard_field == "amount":
                parsed_amount = self._parse_amount(value)
                if parsed_amount is None and row_index is not None:
                    logger.debug(
                        "Row %s (%s) has unparsable amount value '%s' (column '%s').",
                        row_index,
                        source_file,
                        value,
                        csv_column,
                    )
                standardized[standard_field] = parsed_amount
            elif standard_field == "description":
                standardized[standard_field] = self._parse_string(value, max_length=500)
            elif standard_field == "category":
                standardized[standard_field] = self._parse_string(value, max_length=100)
            elif standard_field == "account":
                standardized[standard_field] = self._parse_string(value, max_length=100)
            else:
                standardized[standard_field] = self._parse_string(value)

        return standardized

    def _validate_column_mapping(self, column_mapping: Dict[str, Optional[str]], available_columns: List[str]) -> None:
        """Ensure required fields were mapped successfully."""
        missing_fields = [field for field in self.REQUIRED_FIELDS if column_mapping.get(field) is None]
        if missing_fields:
            raise StandardizationError(
                f"Missing required fields in CSV: {missing_fields}. Available columns: {available_columns}"
            )

    def standardize_dataframe(
        self,
        df: pd.DataFrame,
        source_file: str,
    ) -> List[Dict[str, Any]]:
        """
        Standardize an entire DataFrame of transaction data.

        Args:
            df: Pandas DataFrame containing transaction data.
            source_file: Name/path of the source CSV file.

        Returns:
            List of standardized transaction dictionaries.

        Raises:
            StandardizationError: If required fields are missing or error thresholds exceeded.
        """
        if df.empty:
            logger.warning("Received empty DataFrame for source '%s'; nothing to standardize.", source_file)
            return []

        column_mapping = self.map_columns(df.columns.tolist())
        self._validate_column_mapping(column_mapping, df.columns.tolist())

        standardized_transactions: List[Dict[str, Any]] = []
        skipped_rows = 0
        error_count = 0
        total_rows = len(df)
        error_budget = self._compute_error_budget(total_rows)

        for idx, row in df.iterrows():
            try:
                standardized = self.standardize_row(
                    row,
                    column_mapping,
                    row_index=idx,
                    source_file=source_file,
                )

                if not self._ensure_required_fields(standardized, idx, source_file):
                    skipped_rows += 1
                    error_count += 1
                    if error_budget is not None and error_count > error_budget:
                        message = (
                            f"Exceeded error budget while standardizing '{source_file}' "
                            f"({error_count}/{error_budget} row issues)."
                        )
                        logger.error(message)
                        raise StandardizationError(message)
                    continue

                standardized["source_file"] = source_file
                standardized_transactions.append(standardized)

            except StandardizationError as exc:
                skipped_rows += 1
                error_count += 1
                if error_budget is not None and error_count > error_budget:
                    message = (
                        f"Exceeded error budget while standardizing '{source_file}' "
                        f"({error_count}/{error_budget} row issues)."
                    )
                    logger.error(message)
                    raise StandardizationError(message) from exc
                logger.warning("Error standardizing row %s in '%s': %s", idx, source_file, exc)
                continue

        logger.info(
            "Standardized %s transactions from '%s'; skipped %s rows.",
            len(standardized_transactions),
            source_file,
            skipped_rows,
        )

        return standardized_transactions

    # Added: Chunk-aware standardization helper
    def standardize_stream(
        self,
        dataframe_iterator: Iterable[pd.DataFrame],
        source_file: str,
    ) -> List[Dict[str, Any]]:
        """
        Standardize a stream (iterator) of DataFrame chunks.

        Args:
            dataframe_iterator: Iterable of DataFrame chunks.
            source_file: Name/path of the source CSV file.

        Returns:
            Aggregated list of standardized transaction dictionaries.
        """
        standardized: List[Dict[str, Any]] = []
        for chunk in dataframe_iterator:
            if chunk is None or chunk.empty:
                continue
            standardized.extend(self.standardize_dataframe(chunk, source_file))
        return standardized


