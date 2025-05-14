# import sqlite3
# import pandas as pd

# # Connect and setup
# conn = sqlite3.connect("current.db")
# cursor = conn.cursor()

# # Create tables
# cursor.execute("CREATE TABLE STUDENT (ROLL_NO INT, NAME TEXT, SECTION TEXT)")
# cursor.execute("CREATE TABLE DATABASE (ROLL_NO INT, NAME TEXT, LOCATION TEXT, PHONE_NUMBER TEXT)")

# # Insert test data
# cursor.executemany("INSERT INTO STUDENT VALUES (?, ?, ?)", [
#     (1, 'Alice', 'A'),
#     (2, 'Bob', 'B'),
#     (3, 'Carol', 'A')
# ])
# cursor.executemany("INSERT INTO DATABASE VALUES (?, ?, ?, ?)", [
#     (1, 'Alice', 'New York', '123'),
#     (2, 'Bob', 'Chicago', '456'),
#     (3, 'Carol', 'LA', '789'),
#     (4, 'Dave', 'Boston', '321')
# ])
# conn.commit()

import sqlite3

# --- DYNAMIC SETUP ---
def setup_tables(conn, tables):
    cursor = conn.cursor()
    for table_name, table_info in tables.items():
        schema = table_info['schema']
        data = table_info['data']

        # Create table
        cursor.execute(f"CREATE TABLE {table_name} ({schema})")

        # Insert data
        placeholders = ', '.join(['?'] * len(data[0]))
        cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", data)
    conn.commit()