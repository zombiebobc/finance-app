import os

from cryptography.fernet import Fernet

# Ensure the encryption layer has a deterministic key in test environments so
# config.yaml is not mutated during test runs.
os.environ.setdefault("FINANCE_APP_ENCRYPTION_KEY", Fernet.generate_key().decode("utf-8"))

