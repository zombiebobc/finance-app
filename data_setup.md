# Data Setup Guide

## Introduction

This guide explains how to manage sensitive financial data locally while collaborating on the project safely. Follow these steps to keep real transaction data private, use the application with local datasets, and contribute sanitized samples to the repository.

## Directory Structure

- Create a dedicated directory for private data (e.g., `data/`) at the project root:
  - `mkdir data`
  - Do **not** commit this directory; it is ignored by Git via `.gitignore`.
- Keep the public repository clean by storing only anonymized examples in `samples/` (committed).
- Optional directories for generated artifacts:
  - `exports/` for ad-hoc exports (ignored).
  - `logs/` for verbose application logs (ignored).

```
finance-app/
├── config.yaml              # Shared template committed to the repo
├── config.local.yaml        # Personal overrides (ignored)
├── data/                    # Real financial data (ignored)
├── samples/                 # Sanitized sample data (committed)
├── exports/                 # Generated reports/exports (ignored)
└── ...
```

## Configuration

- Use the committed `config.yaml` as a template. Keep shared, non-secret defaults here.
- For machine-specific or secret settings, create `config.local.yaml` (ignored) and pass it via CLI:
  - `python main.py --config config.local.yaml import --file data/transactions.csv`
- Alternatively, configure paths with environment variables:
  - `FINANCE_APP_DATA_DIR=./data`
  - `FINANCE_APP_DB_PATH=./data/transactions.db`
  - `FINANCE_APP_CONFIG=./config.local.yaml`
- You can also provide data paths via command-line options:
  - `python main.py import --file data/bank/2025-01-transactions.csv`
- When running scripts, confirm the application uses the private directory before executing imports.

## Sample Data

- Provide sanitized CSV samples for documentation and tests in `samples/`:
  - Remove or anonymize personally identifiable information (PII).
  - Aggregate or round sensitive amounts if necessary.
  - Use fake account numbers and descriptions.
- Reference samples in README examples to ensure reproducible docs without exposing real data.
- Consider providing companion metadata that explains how samples were generated for transparency.

## Best Practices

- Never place real data outside ignored directories (`data/`, `logs/`, `exports/`).
- Before committing, double-check staged files:
  - Run `git status` and `git diff --staged`.
  - Use `git add -n <path>` for a dry run to preview staged changes without adding real data.
- Avoid piping sensitive CLI output into tracked files.
- When sharing debugging information, scrub logs and replace amounts/descriptions with placeholders.
- Integrate with the existing import workflow safely:
  - `python main.py import --file data/private/transactions.csv`
  - Add `--dry-run` or custom safeguards if you are testing new parsers.
- Regularly review `.gitignore` to ensure new file types produced by tooling remain private.
- If you accidentally commit sensitive data, rotate exposed secrets, purge the data from Git history (`git filter-repo`), and force push after coordinating with the team.


