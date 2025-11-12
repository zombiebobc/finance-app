## Error Handling Update Summary

- Added `IngestionError` and `StandardizationError` custom exceptions in `utils.py`, alongside a shared `prompt_user_choice` helper to centralize CLI decision making and enable dependency injection for tests.
- Hardened `data_ingestion.CSVReader` to auto-detect delimiter, retry multiple pandas read configurations, support auto chunking by file size, and decide between skipping, prompting, or raising on failures without crashing the import pipeline.
- Enhanced `data_standardization.DataStandardizer` with configurable error thresholds, fallback values, and interactive remediation so malformed rows can be skipped or repaired while keeping imports resilient.
- Extended configuration (`config.yaml`) with processing knobs (`auto_chunk_mb`, `error_ratio`, `max_error_rows`, `fallback_values`) and wired them through `main.py` and `enhanced_import.py` to ensure the new safeguards are active across CLI workflows.
- Introduced regression tests in `tests/test_ingestion.py` covering missing files, malformed CSVs, chunked ingestion, missing required mappings, and fallback prompting, safeguarding the new behaviour.

