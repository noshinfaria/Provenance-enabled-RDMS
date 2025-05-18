import sqlite3
from store_data import setup_tables, print_all_data
from provenance import run_query_and_provenance
tables = {
    'STUDENT': {
        'schema': 'ROLL_NO INT, NAME TEXT, SECTION TEXT',
        'data': [
            (1, 'Alice', 'A'),
            (2, 'Bob', 'B'),
            (3, 'Carol', 'A')
        ]
    },
    'DATABASE': {
        'schema': 'ROLL_NO INT, NAME TEXT, LOCATION TEXT, PHONE_NUMBER TEXT',
        'data': [
            (1, 'Alice', 'New York', '123'),
            (2, 'Bob', 'Chicago', '456'),
            (3, 'Carol', 'LA', '789'),
            (4, 'Dave', 'Boston', '321')
        ]
    }
}

conn = sqlite3.connect('example.db')
setup_tables(conn, tables)
print_all_data(conn)


# --- Test query ---
query = """
SELECT NAME, LOCATION, PHONE_NUMBER 
FROM DATABASE 
WHERE ROLL_NO IN (
    SELECT ROLL_NO FROM STUDENT WHERE SECTION = 'A'
);
"""

# Run
result, provenance = run_query_and_provenance(query)

# --- OUTPUT ---
print("\nStep 8: Final Output\n---------------------------")
print("Query Results:")
for r in result:
    print(r)

print("\nProvenance:")
for res_row, prov_rows in provenance:
    print(f"Result Row: {res_row}, Derived from: {prov_rows}")