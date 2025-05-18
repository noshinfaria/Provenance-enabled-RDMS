import sqlite3
import sqlparse

def run_query_and_provenance(query):
    print("\nStep 1: Connecting to the database and executing the main query...")
    conn = sqlite3.connect('example.db')
    cursor = conn.cursor()

    # Execute the query and fetch results
    cursor.execute(query)
    query_results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    # Parse SQL using sqlparse
    print("\nStep 2: Parsing SQL query using sqlparse...")
    parsed = sqlparse.parse(query)[0]
    tokens = [t for t in parsed.tokens if not t.is_whitespace]
    print("Tokens parsed from SQL:")
    for i, t in enumerate(tokens):
        print(f"  Token {i}: {repr(t)}")

    # Extract provenance details
    print("\nStep 3: Extracting tables and fields for provenance tracking...")
    outer_table = None
    subquery_table = None
    match_field = None
    condition_field = None
    condition_value = None

    # Find outer table
    for i, token in enumerate(tokens):
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
            outer_table = tokens[i + 1].value.strip()
            print(f"  Found outer table: {outer_table}")
            break

    # Find subquery tokens
    for token in tokens:
        if 'IN (' in str(token):
            print("  Found subquery inside IN (...)")
            inner = str(token).split('IN (')[1].rstrip('); ')
            inner_tokens = inner.split('FROM')

            match_field = inner_tokens[0].replace('SELECT', '').strip()
            print(f"    Match field from subquery SELECT: {match_field}")
            subquery_table_and_where = inner_tokens[1].strip()

            if 'WHERE' in subquery_table_and_where:
                subquery_table = subquery_table_and_where.split('WHERE')[0].strip()
                condition_part = subquery_table_and_where.split('WHERE')[1].strip()

                if '=' in condition_part:
                    condition_field, condition_value = condition_part.split('=')
                    condition_field = condition_field.strip()
                    condition_value = condition_value.strip().strip("'")
                print(f"    Subquery table: {subquery_table}")
                print(f"    Condition: {condition_field} = {condition_value}")
            else:
                subquery_table = subquery_table_and_where
                print(f"    Subquery table (no WHERE clause): {subquery_table}")

    # Validate parsing
    print("Parsed SQL Details:")
    print("\nStep 4: Validating parsed components...")
    print(f"  Outer Table: {outer_table}")
    print(f"  Subquery Table: {subquery_table}")
    print(f"  Match Field: {match_field}")
    print(f"  Condition Field: {condition_field}")
    print(f"  Condition Value: {condition_value}")

    if not all([outer_table, subquery_table, match_field, condition_field, condition_value]):
        raise ValueError("Failed to extract required provenance details from SQL query.")

    # Get matched values from subquery
    print("\nStep 5: Running subquery to get matching values...")
    cursor.execute(f"SELECT {match_field} FROM {subquery_table} WHERE {condition_field} = ?", (condition_value,))
    matched_ids = {row[0] for row in cursor.fetchall()}
    print(f"  Matched IDs from subquery: {matched_ids}")

    # Get all rows from outer table
    print("\nStep 6: Fetching all rows from outer table...")
    cursor.execute(f"SELECT * FROM {outer_table}")
    all_outer_rows = cursor.fetchall()
    outer_columns = [col[1] for col in cursor.execute(f"PRAGMA table_info({outer_table})")]
    print(f"  Outer table columns: {outer_columns}")
    print(f"  Total rows in outer table: {len(outer_columns)}")

    # Build provenance
    print("\nStep 7: Filtering results and building provenance...")
    final_results = []
    provenance = []

    for row in all_outer_rows:
        row_dict = dict(zip(outer_columns, row))
        if row_dict.get(match_field) in matched_ids:
            final_results.append(row)
            print(f"  Matched row: {row}")

            cursor.execute(
                f"SELECT * FROM {subquery_table} WHERE {match_field} = ?",
                (row_dict[match_field],)
            )
            prov_rows = cursor.fetchall()
            print(f"     ↳ Provenance from subquery: {prov_rows}")
            provenance.append((row, prov_rows))

    # Return output
    return final_results, provenance







# --- RUN QUERY & TRACK PROVENANCE ---
# def run_query_and_provenance(conn, outer_table, subquery_table, match_field, subquery_condition_field, subquery_condition_value):
#     cursor = conn.cursor()

#     # Subquery: get matching values from subquery table
#     cursor.execute(f"SELECT {match_field} FROM {subquery_table} WHERE {subquery_condition_field}=?", (subquery_condition_value,))
#     matched_ids = {row[0] for row in cursor.fetchall()}

#     # Outer query: get all rows
#     cursor.execute(f"SELECT * FROM {outer_table}")
#     all_outer_rows = cursor.fetchall()

#     # Provenance results
#     query_results = []
#     provenance = []

#     for row in all_outer_rows:
#         row_dict = dict(zip([col[0] for col in cursor.execute(f"PRAGMA table_info({outer_table})")], row))
#         if row_dict[match_field] in matched_ids:
#             query_results.append(row)

#             # Provenance lookup
#             cursor.execute(f"SELECT * FROM {subquery_table} WHERE {match_field}=?", (row_dict[match_field],))
#             prov_rows = cursor.fetchall()

#             provenance.append((row, prov_rows))

#     return query_results, provenance