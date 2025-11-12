"""
Additional ingestion and standardization regression tests focusing on error handling.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from data_ingestion import CSVReader
from data_standardization import DataStandardizer
from utils import IngestionError, StandardizationError


@pytest.fixture
def sample_mappings() -> dict:
    """Common column mappings used across standardization tests."""
    return {
        "date": ["date", "transaction date"],
        "description": ["description", "transaction description"],
        "amount": ["amount", "transaction amount"],
        "category": ["category"],
    }


def test_read_csv_missing_file_raises_ingestion_error():
    """Ensure missing files raise custom ingestion errors when strict mode is enabled."""
    reader = CSVReader(skip_on_error=False)

    with pytest.raises(IngestionError):
        reader.read_csv(Path("this-file-does-not-exist.csv"), on_error="raise")


def test_read_csv_malformed_file_skips_when_configured():
    """Malformed CSVs should log and return an empty dataframe when skipping on error."""
    bad_content = 'Date,Description,Amount\n2024-01-01,"Groceries,-45.00\n'
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
        tmp.write(bad_content)
        tmp_path = Path(tmp.name)

    try:
        reader = CSVReader(skip_on_error=True)
        df = reader.read_csv(tmp_path, on_error="skip")
        assert isinstance(df, pd.DataFrame)
        assert df.empty
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def test_chunked_reading_returns_iterator(tmp_path: Path):
    """Large files should be processed in chunks to reduce memory pressure."""
    record_count = 25
    df = pd.DataFrame(
        {
            "Date": ["2024-01-{:02d}".format(i + 1) for i in range(record_count)],
            "Description": [f"Item {i}" for i in range(record_count)],
            "Amount": [float(i) for i in range(record_count)],
        }
    )
    csv_path = tmp_path / "large.csv"
    df.to_csv(csv_path, index=False)

    reader = CSVReader(chunk_size=10)
    iterator = reader.read_csv(csv_path, chunked=True, on_error="raise")

    total_rows = 0
    for chunk in iterator:
        assert len(chunk) <= 10
        total_rows += len(chunk)

    assert total_rows == record_count


def test_standardize_dataframe_missing_required_column(sample_mappings):
    """Missing required column mappings should raise StandardizationError."""
    df = pd.DataFrame({"Date": ["2024-01-01"], "Description": ["Test entry"]})
    standardizer = DataStandardizer(sample_mappings, ["%Y-%m-%d"])

    with pytest.raises(StandardizationError):
        standardizer.standardize_dataframe(df, "missing_amount.csv")


def test_standardize_dataframe_applies_fallback_prompt(sample_mappings):
    """Invalid amount entries should leverage configured fallbacks via prompts."""
    df = pd.DataFrame(
        {
            "Date": ["2024-01-01"],
            "Description": ["Utility bill"],
            "Amount": ["not-a-number"],
        }
    )

    def always_default_prompt(*_args, **_kwargs) -> str:
        return "d"

    standardizer = DataStandardizer(
        sample_mappings,
        ["%Y-%m-%d"],
        fallback_values={"amount": 0.0},
        prompt_handler=always_default_prompt,
    )

    standardized = standardizer.standardize_dataframe(df, "fallback.csv")
    assert len(standardized) == 1
    assert standardized[0]["amount"] == 0.0
    assert standardized[0]["description"] == "Utility bill"

