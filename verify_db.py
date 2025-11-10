"""Quick script to verify database contents."""
from database_ops import DatabaseManager, Transaction

db = DatabaseManager('sqlite:///transactions.db')
session = db.get_session()

transactions = session.query(Transaction).limit(10).all()

print("\n" + "=" * 90)
print("SAMPLE TRANSACTIONS FROM DATABASE")
print("=" * 90)
print(f"{'Date':<12} | {'Description':<40} | {'Amount':>12} | {'Category':<20}")
print("-" * 90)

for t in transactions:
    print(f"{t.date.date()} | {t.description[:40]:40s} | ${t.amount:10.2f} | {t.category or 'N/A':20s}")

print("=" * 90)
print(f"\nTotal transactions in database: {session.query(Transaction).count()}")
print(f"Source files: {set(t.source_file for t in session.query(Transaction).all())}")

session.close()
db.close()

