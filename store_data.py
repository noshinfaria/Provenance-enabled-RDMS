import sqlite3
from datetime import datetime

# Connect to SQLite database (creates a new file if not exists)
conn = sqlite3.connect("example.db")
cursor = conn.cursor()

# Drop table if it exists
cursor.execute("DROP TABLE IF EXISTS price_audit")

# Create price_audit table
cursor.execute("""
CREATE TABLE price_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    old_price REAL NOT NULL,
    new_price REAL NOT NULL,
    operation_time TEXT NOT NULL
)
""")

# Insert sample data
sample_data = [
    (765, 100.00, 95.00, '2022-01-15 10:00:00'),
    (765, 95.00, 90.00, '2022-03-10 14:30:00'),
    (765, 90.00, 85.00, '2022-05-01 08:00:00'),
    (765, 85.00, 90.00, '2022-07-01 11:00:00'),
    (123, 120.00, 110.00, '2022-01-10 09:15:00'),
    (765, 90.00, 88.00, '2023-02-01 16:45:00')
]

cursor.executemany("""
INSERT INTO price_audit (product_id, old_price, new_price, operation_time)
VALUES (?, ?, ?, ?)
""", sample_data)

conn.commit()
print("Table created and data inserted.")

# Optional: fetch and print to verify
cursor.execute("SELECT * FROM price_audit")
for row in cursor.fetchall():
    print(row)

# Close connection
cursor.close()
conn.close()
