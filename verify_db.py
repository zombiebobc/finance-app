"""Quick script to verify database contents."""

from pathlib import Path
import yaml

from database_ops import DatabaseManager, Transaction
from utils import ensure_data_dir, resolve_connection_string


def _load_config() -> dict:
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path, "r") as handle:
            return yaml.safe_load(handle) or {}
    return {}


def main() -> None:
    """Print a quick sample from the configured database."""
    config = _load_config()
    ensure_data_dir(config)
    connection_string = resolve_connection_string(config)
    
    db = DatabaseManager(connection_string)
    session = db.get_session()
    
    transactions = session.query(Transaction).limit(10).all()
    
    print("\n" + "=" * 90)
    print("SAMPLE TRANSACTIONS FROM DATABASE")
    print("=" * 90)
    print(f"{'Date':<12} | {'Description':<40} | {'Amount':>12} | {'Category':<20}")
    print("-" * 90)
    
    for trans in transactions:
        print(
            f"{trans.date.date()} | "
            f"{trans.description[:40]:40s} | "
            f"${trans.amount:10.2f} | "
            f"{trans.category or 'N/A':20s}"
        )
    
    print("=" * 90)
    print(f"\nTotal transactions in database: {session.query(Transaction).count()}")
    print(f"Source files: {set(t.source_file for t in session.query(Transaction).all())}")
    
    session.close()


if __name__ == "__main__":
    main()
