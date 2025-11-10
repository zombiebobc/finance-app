"""
Data ingestion module for reading and parsing CSV files.

This module handles reading CSV files in various formats, detecting formats,
and extracting data using pandas. Supports chunked processing for large files.
"""

import logging
from pathlib import Path
from typing import List, Iterator, Optional, Union
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)


class CSVReader:
    """
    Reads and parses CSV files with various formats.
    
    Handles:
    - Different CSV formats and delimiters
    - Missing or malformed headers
    - Inconsistent rows
    - Large files (chunked processing)
    """
    
    def __init__(self, chunk_size: int = 10000):
        """
        Initialize the CSV reader.
        
        Args:
            chunk_size: Number of rows to process per chunk (for large files)
        """
        self.chunk_size = chunk_size
        logger.info(f"CSV reader initialized with chunk size: {chunk_size}")
    
    def _detect_delimiter(self, file_path: Path, sample_lines: int = 5) -> str:
        """
        Detect the CSV delimiter by analyzing sample lines.
        
        Args:
            file_path: Path to the CSV file
            sample_lines: Number of lines to sample for detection
        
        Returns:
            Detected delimiter character (default: ',')
        """
        common_delimiters = [',', ';', '\t', '|']
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sample = ''.join([f.readline() for _ in range(sample_lines)])
            
            delimiter_counts = {delim: sample.count(delim) for delim in common_delimiters}
            
            if delimiter_counts:
                detected = max(delimiter_counts, key=delimiter_counts.get)
                if delimiter_counts[detected] > 0:
                    logger.debug(f"Detected delimiter: '{detected}'")
                    return detected
        except Exception as e:
            logger.warning(f"Failed to detect delimiter: {e}")
        
        # Default to comma
        return ','
    
    def read_csv(
        self,
        file_path: Path,
        chunked: bool = False
    ) -> Union[Iterator[pd.DataFrame], pd.DataFrame]:
        """
        Read a CSV file and return DataFrame(s).
        
        Args:
            file_path: Path to the CSV file
            chunked: If True, return iterator of DataFrames (for large files)
                    If False, return single DataFrame
        
        Returns:
            DataFrame or Iterator of DataFrames
        
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file cannot be parsed
        """
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        logger.info(f"Reading CSV file: {file_path}")
        
        # Detect delimiter
        delimiter = self._detect_delimiter(file_path)
        
        try:
            # Prepare common read_csv parameters
            # Use 'c' engine by default (faster, supports low_memory)
            # Fall back to 'python' engine if needed
            read_params = {
                'delimiter': delimiter,
                'encoding': 'utf-8',
                'low_memory': False
            }
            
            # Add on_bad_lines parameter if available (pandas 1.3+)
            # Will fallback to error_bad_lines for older versions if needed
            read_params['on_bad_lines'] = 'skip'
            
            if chunked:
                # Return iterator for chunked reading
                read_params['chunksize'] = self.chunk_size
                try:
                    return pd.read_csv(file_path, **read_params)
                except (TypeError, ValueError) as e:
                    # Fallback: try with python engine or remove problematic params
                    if 'on_bad_lines' in read_params:
                        read_params.pop('on_bad_lines', None)
                        read_params['error_bad_lines'] = False
                    if 'low_memory' in read_params:
                        read_params.pop('low_memory', None)
                    read_params['engine'] = 'python'
                    return pd.read_csv(file_path, **read_params)
            else:
                # Read entire file
                try:
                    df = pd.read_csv(file_path, **read_params)
                except (TypeError, ValueError) as e:
                    # Fallback: try with python engine or remove problematic params
                    if 'on_bad_lines' in read_params:
                        read_params.pop('on_bad_lines', None)
                        read_params['error_bad_lines'] = False
                    if 'low_memory' in read_params:
                        read_params.pop('low_memory', None)
                    read_params['engine'] = 'python'
                    df = pd.read_csv(file_path, **read_params)
                
                logger.info(f"Successfully read {len(df)} rows from CSV file")
                return df
                
        except pd.errors.EmptyDataError:
            logger.error(f"CSV file is empty: {file_path}")
            raise ValueError(f"CSV file is empty: {file_path}")
        except Exception as e:
            logger.error(f"Failed to read CSV file: {e}")
            raise ValueError(f"Failed to read CSV file: {e}") from e
    
    def validate_csv(self, file_path: Path) -> tuple[bool, Optional[str]]:
        """
        Validate that a file is a readable CSV.
        
        Args:
            file_path: Path to the CSV file
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not file_path.exists():
            return False, f"File not found: {file_path}"
        
        if not file_path.is_file():
            return False, f"Path is not a file: {file_path}"
        
        if file_path.suffix.lower() != '.csv':
            logger.warning(f"File does not have .csv extension: {file_path}")
            # Continue anyway, as some CSV files might not have the extension
        
        try:
            # Try to read first few rows
            df = self.read_csv(file_path)
            if isinstance(df, pd.DataFrame) and len(df) == 0:
                return False, "CSV file is empty"
            
            # Check if it has any columns
            if isinstance(df, pd.DataFrame) and len(df.columns) == 0:
                return False, "CSV file has no columns"
            
            return True, None
            
        except Exception as e:
            return False, f"Failed to read CSV: {str(e)}"
    
    def get_file_info(self, file_path: Path) -> dict:
        """
        Get information about a CSV file.
        
        Args:
            file_path: Path to the CSV file
        
        Returns:
            Dictionary with file information (rows, columns, size, etc.)
        """
        info = {
            "path": str(file_path),
            "name": file_path.name,
            "size_bytes": file_path.stat().st_size if file_path.exists() else 0,
            "rows": 0,
            "columns": [],
            "valid": False
        }
        
        try:
            df = self.read_csv(file_path)
            if isinstance(df, pd.DataFrame):
                info["rows"] = len(df)
                info["columns"] = df.columns.tolist()
                info["valid"] = True
        except Exception as e:
            logger.warning(f"Failed to get file info: {e}")
        
        return info

