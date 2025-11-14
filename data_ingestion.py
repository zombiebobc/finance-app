"""
Data ingestion module for reading and parsing CSV files.

This module handles reading CSV files in various formats, detecting formats,
and extracting data using pandas. Supports chunked processing for large files.
"""

from __future__ import annotations

import logging
from io import BytesIO
from pathlib import Path
from typing import Callable, Dict, Iterator, Optional, Union

import pandas as pd
from pandas import errors as pd_errors

# Import custom exception and prompt helper for resilience
from exceptions import IngestionError
from utils import prompt_user_choice

# Configure logging
logger = logging.getLogger(__name__)


PromptHandler = Callable[[str, Dict[str, str], str], str]


class CSVReader:
    """
    Reads and parses CSV files with various formats while handling edge cases.

    Handles:
    - Different CSV formats and delimiters
    - Missing or malformed headers
    - Inconsistent rows
    - Large files (chunked processing)
    - Graceful handling of malformed files with user-guided fallbacks
    """

    def __init__(
        self,
        chunk_size: int = 10000,
        *,
        auto_chunk_mb: int = 25,
        prompt_handler: Optional[PromptHandler] = None,
        skip_on_error: bool = True
    ):
        """
        Initialize the CSV reader.

        Args:
            chunk_size: Number of rows to process per chunk (for large files).
            auto_chunk_mb: File size threshold (MB) to automatically enable chunked processing.
            prompt_handler: Optional callable to resolve ingestion decisions interactively.
            skip_on_error: When True, malformed files yield empty results instead of raising.
        """
        self.chunk_size = chunk_size
        self.auto_chunk_mb = auto_chunk_mb
        self.prompt_handler = prompt_handler or prompt_user_choice
        self.skip_on_error = skip_on_error
        logger.info("CSV reader initialized with chunk size %s and auto-chunk threshold %sMB", chunk_size, auto_chunk_mb)

    def _detect_delimiter(self, file_path: Path, sample_lines: int = 5) -> str:
        """
        Detect the CSV delimiter by analyzing sample lines.

        Args:
            file_path: Path to the CSV file.
            sample_lines: Number of lines to sample for detection.

        Returns:
            Detected delimiter character (default: ',').
        """
        common_delimiters = [',', ';', '\t', '|']

        try:
            with open(file_path, 'r', encoding='utf-8') as file_handle:
                sample = ''.join([file_handle.readline() for _ in range(sample_lines)])

            delimiter_counts = {delim: sample.count(delim) for delim in common_delimiters}

            if delimiter_counts:
                detected = max(delimiter_counts, key=delimiter_counts.get)
                if delimiter_counts[detected] > 0:
                    logger.debug("Detected delimiter '%s' for file %s", detected, file_path)
                    return detected
        except OSError as exc:
            logger.warning("Failed to detect delimiter for '%s': %s", file_path, exc)

        # Default to comma
        return ','

    def _should_chunk(self, file_path: Path, chunked_hint: Optional[bool]) -> bool:
        """Determine if chunked reading should be used."""
        if chunked_hint is not None:
            return chunked_hint
        try:
            size_bytes = file_path.stat().st_size
            use_chunking = size_bytes >= self.auto_chunk_mb * 1024 * 1024
            if use_chunking:
                logger.debug("Auto-enabled chunked processing for '%s' (%s bytes).", file_path, size_bytes)
            return use_chunking
        except OSError as exc:
            logger.debug("Unable to determine file size for '%s': %s", file_path, exc)
            return False

    def _build_base_params(self, delimiter: str) -> Dict[str, Union[str, bool, int]]:
        """Prepare baseline pandas.read_csv parameters."""
        return {
            "delimiter": delimiter,
            "encoding": "utf-8",
            "low_memory": False,
            "on_bad_lines": "skip",
        }

    def _yield_empty_iterator(self) -> Iterator[pd.DataFrame]:
        """Return an empty iterator for chunked operations."""
        return iter(())

    def _handle_read_failure(
        self,
        file_path: Path,
        error: Exception,
        *,
        chunked: bool,
        on_error: str
    ) -> Union[Iterator[pd.DataFrame], pd.DataFrame]:
        """Centralized handler for read failures with prompt support."""
        message = f"Failed to ingest '{file_path}': {error}"
        logger.error(message)

        action = on_error
        if action not in {"raise", "skip", "prompt"}:
            logger.warning("Unknown on_error action '%s'. Falling back to 'raise'.", action)
            action = "raise"

        if action == "prompt":
            selection = self.prompt_handler(
                f"{message}. How would you like to proceed?",
                {"s": "Skip file", "r": "Retry with safe defaults", "a": "Abort import"},
                default="s",
            )
            if selection == "r":
                logger.info("Retrying file '%s' with conservative fallback parameters.", file_path)
                return self._read_with_variants(file_path, self._build_base_params(','), chunked, allow_strict_fallback=True)
            if selection == "a":
                raise IngestionError(message) from error
            action = "skip"

        if action == "skip" or (action == "raise" and self.skip_on_error):
            logger.warning("Skipping file '%s' due to ingestion failure.", file_path)
            return self._yield_empty_iterator() if chunked else pd.DataFrame()

        raise IngestionError(message) from error

    def _read_with_variants(
        self,
        file_path: Path,
        base_params: Dict[str, Union[str, bool, int]],
        chunked: bool,
        allow_strict_fallback: bool = False
    ) -> Union[Iterator[pd.DataFrame], pd.DataFrame]:
        """Attempt to read a CSV file using a series of fallback parameter sets."""
        params_attempts = []

        base = dict(base_params)
        if chunked:
            base["chunksize"] = self.chunk_size
        params_attempts.append(base)

        # Added: Alternate encoding fallback
        latin_params = dict(base)
        latin_params["encoding"] = "latin-1"
        params_attempts.append(latin_params)

        # Added: Python engine fallback for tricky CSVs
        python_params = dict(base)
        python_params["engine"] = "python"
        python_params.setdefault("on_bad_lines", "skip")
        params_attempts.append(python_params)

        if allow_strict_fallback:
            fallback = dict(base)
            fallback.pop("on_bad_lines", None)
            fallback["engine"] = "python"
            fallback["dtype"] = str
            params_attempts.append(fallback)

        last_error: Optional[Exception] = None
        for params in params_attempts:
            try:
                logger.debug("Attempting to read '%s' with params: %s", file_path, params)
                return pd.read_csv(file_path, **params)
            except (UnicodeDecodeError, pd_errors.ParserError, pd_errors.EmptyDataError, OSError, ValueError) as exc:
                last_error = exc
                logger.debug("Read attempt failed for '%s' with params %s: %s", file_path, params, exc)
                continue

        if last_error is None:
            last_error = IngestionError(f"Unknown error ingesting '{file_path}'")

        raise IngestionError(f"Failed to read CSV '{file_path}'") from last_error

    def read_csv(
        self,
        file_path: Path,
        *,
        chunked: Optional[bool] = None,
        on_error: str = "skip"
    ) -> Union[Iterator[pd.DataFrame], pd.DataFrame]:
        """
        Read a CSV file and return DataFrame(s) with robust error handling.

        Args:
            file_path: Path to the CSV file.
            chunked: If True, return iterator of DataFrames (for large files). When None, auto-detect
                based on file size.
            on_error: Controls behavior when parsing fails. Options: 'skip', 'raise', 'prompt'.

        Returns:
            DataFrame or iterator of DataFrames depending on the chunked flag.

        Raises:
            IngestionError: If reading fails and on_error='raise'.
        """
        if not file_path.exists():
            message = f"CSV file not found: {file_path}"
            logger.error(message)
            raise IngestionError(message)

        logger.info("Reading CSV file: %s", file_path)

        delimiter = self._detect_delimiter(file_path)
        base_params = self._build_base_params(delimiter)
        use_chunking = self._should_chunk(file_path, chunked)

        try:
            result = self._read_with_variants(file_path, base_params, use_chunking)
        except IngestionError as exc:
            return self._handle_read_failure(file_path, exc, chunked=use_chunking, on_error=on_error)

        if isinstance(result, pd.DataFrame):
            logger.info("Successfully read %s rows from '%s'", len(result), file_path)
        else:
            logger.info("Created chunk iterator for '%s' (chunk size: %s)", file_path, self.chunk_size)

        return result

    def validate_csv(self, file_path: Path) -> tuple[bool, Optional[str]]:
        """
        Validate that a file is a readable CSV.

        Args:
            file_path: Path to the CSV file.

        Returns:
            Tuple of (is_valid, error_message).
        """
        if not file_path.exists():
            return False, f"File not found: {file_path}"

        if not file_path.is_file():
            return False, f"Path is not a file: {file_path}"

        if file_path.suffix.lower() != '.csv':
            logger.warning("File does not have .csv extension: %s", file_path)

        try:
            preview = self.read_csv(file_path, chunked=False, on_error="raise")
            if isinstance(preview, pd.DataFrame) and preview.empty:
                return False, "CSV file is empty"

            if isinstance(preview, pd.DataFrame) and len(preview.columns) == 0:
                return False, "CSV file has no columns"

            return True, None

        except IngestionError as exc:
            return False, f"Failed to read CSV: {exc}"

    def get_file_info(self, file_path: Path) -> dict:
        """
        Get information about a CSV file.

        Args:
            file_path: Path to the CSV file.

        Returns:
            Dictionary with file information (rows, columns, size, etc.).
        """
        info = {
            "path": str(file_path),
            "name": file_path.name,
            "size_bytes": file_path.stat().st_size if file_path.exists() else 0,
            "rows": 0,
            "columns": [],
            "valid": False,
        }

        try:
            df_or_iter = self.read_csv(file_path, chunked=False, on_error="skip")
            if isinstance(df_or_iter, pd.DataFrame):
                info["rows"] = len(df_or_iter)
                info["columns"] = df_or_iter.columns.tolist()
                info["valid"] = not df_or_iter.empty
        except IngestionError as exc:
            logger.warning("Failed to get file info for '%s': %s", file_path, exc)

        return info


def preview_csv(
    file_obj: BytesIO,
    *,
    max_rows: int = 10,
    encoding: str = "utf-8"
) -> pd.DataFrame:
    """
    Quickly parse the first few rows of an in-memory CSV upload.
    
    Args:
        file_obj: BytesIO-like object returned by Streamlit's uploader.
        max_rows: Maximum number of rows to read for the preview.
        encoding: Preferred text encoding; falls back to latin-1 automatically.
    
    Returns:
        Pandas DataFrame limited to ``max_rows`` rows.
    
    Raises:
        IngestionError: If the CSV cannot be parsed safely.
        ValueError: If ``file_obj`` is not seekable.
    """
    if max_rows <= 0:
        raise ValueError("preview_csv requires max_rows > 0")
    
    if not hasattr(file_obj, "read"):
        raise ValueError("preview_csv expects a file-like object supporting read/seek.")
    
    if not hasattr(file_obj, "seek"):
        raise ValueError("preview_csv requires a seekable in-memory file object.")
    
    try:
        file_obj.seek(0)
        df = pd.read_csv(file_obj, nrows=max_rows, encoding=encoding)
    except UnicodeDecodeError:
        file_obj.seek(0)
        try:
            df = pd.read_csv(file_obj, nrows=max_rows, encoding="latin-1")
        except Exception as exc:
            raise IngestionError(f"Failed to preview CSV with fallback encoding: {exc}") from exc
    except (pd_errors.ParserError, pd_errors.EmptyDataError, ValueError) as exc:
        raise IngestionError(f"Failed to preview CSV: {exc}") from exc
    except Exception as exc:  # pragma: no cover - defensive catch-all
        raise IngestionError(f"Unexpected error while previewing CSV: {exc}") from exc
    finally:
        file_obj.seek(0)
    
    if df.empty:
        raise IngestionError("CSV preview returned no rows.")
    
    return df.head(max_rows)


