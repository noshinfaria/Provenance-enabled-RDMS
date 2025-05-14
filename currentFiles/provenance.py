# import sqlite3

# conn = sqlite3.connect("current.db")
# cursor = conn.cursor()

# # Get roll numbers from STUDENT where SECTION='A'
# cursor.execute("SELECT ROLL_NO FROM STUDENT WHERE SECTION='A'")
# student_rows = cursor.fetchall()
# roll_nos = {r[0] for r in student_rows}

# # Get the actual result from DATABASE
# cursor.execute("SELECT * FROM DATABASE")
# all_database_rows = cursor.fetchall()

# # Filter by roll_nos and track provenance
# results = []
# provenance = []

# for row in all_database_rows:
#     if row[0] in roll_nos:
#         results.append(row)
#         # Get matching STUDENT rows for provenance
#         cursor.execute("SELECT * FROM STUDENT WHERE ROLL_NO = ?", (row[0],))
#         provenance_data = cursor.fetchall()
#         provenance.append({
#             'result_row': row,
#             'provenance_from_student': provenance_data
#         })

# # Display
# print("Query Results:")
# for row in results:
#     print(row)

# print("\nProvenance:")
# for p in provenance:
#     print(f"Result Row: {p['result_row']}, Derived from: {p['provenance_from_student']}")


# --- RUN QUERY & TRACK PROVENANCE ---
def run_query_and_provenance(conn, outer_table, subquery_table, match_field, subquery_condition_field, subquery_condition_value):
    cursor = conn.cursor()

    # Subquery: get matching values from subquery table
    cursor.execute(f"SELECT {match_field} FROM {subquery_table} WHERE {subquery_condition_field}=?", (subquery_condition_value,))
    matched_ids = {row[0] for row in cursor.fetchall()}

    # Outer query: get all rows
    cursor.execute(f"SELECT * FROM {outer_table}")
    all_outer_rows = cursor.fetchall()

    # Provenance results
    query_results = []
    provenance = []

    for row in all_outer_rows:
        row_dict = dict(zip([col[0] for col in cursor.execute(f"PRAGMA table_info({outer_table})")], row))
        if row_dict[match_field] in matched_ids:
            query_results.append(row)

            # Provenance lookup
            cursor.execute(f"SELECT * FROM {subquery_table} WHERE {match_field}=?", (row_dict[match_field],))
            prov_rows = cursor.fetchall()

            provenance.append((row, prov_rows))

    return query_results, provenance