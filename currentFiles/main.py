import sqlite3
from store_data import setup_tables
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

conn = sqlite3.connect(':memory:')
setup_tables(conn, tables)

# Customize fields
outer_table = 'DATABASE'
subquery_table = 'STUDENT'
match_field = 'ROLL_NO'
subquery_condition_field = 'SECTION'
subquery_condition_value = 'A'

result, provenance = run_query_and_provenance(
    conn, outer_table, subquery_table, match_field, subquery_condition_field, subquery_condition_value
)

# --- OUTPUT ---
print("Query Results:")
for r in result:
    print(r)

print("\nProvenance:")
for res_row, prov_rows in provenance:
    print(f"Result Row: {res_row}, Derived from: {prov_rows}")