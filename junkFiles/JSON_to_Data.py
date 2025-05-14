import sqlite3

def build_provenance_query(params):
    table = params.get("table")
    since = params.get("since")
    until = params.get("until")
    returns = params.get("return", [])
    filters = params.get("filter", {})
    joins = params.get("join", [])
    order = params.get("order", [])

    sql_parts = []
    alias_map = {}
    all_tables = [table] + [j["TABLE"] for j in joins]
    for i, tbl in enumerate(all_tables):
        alias_map[tbl] = f"t{i+1}"

    main_alias = alias_map[table]

    # SELECT
    select_clause = []
    # if aggregate:
    #     func = aggregate["FUNCTION"]
    #     col = aggregate["ON"]
    #     agg_alias = aggregate["AS"]
    #     select_clause.append(f"{func}({alias_map[col.split('.')[0]]}.{col.split('.')[1]}) AS {agg_alias}")
    for col in returns:
        tbl_name, col_name = col.split(".")
        select_clause.append(f"{alias_map[tbl_name]}.{col_name}")
    sql_parts.append("SELECT " + ", ".join(select_clause))

    # FROM + JOIN
    sql_parts.append(f"FROM {table} {main_alias}")
    for join in joins:
        join_tbl = join["TABLE"]
        join_alias = alias_map[join_tbl]
        join_on = join["ON"]
        for tbl in alias_map:
            join_on = join_on.replace(f"{tbl}.", f"{alias_map[tbl]}.")
        sql_parts.append(f"JOIN {join_tbl} {join_alias} ON {join_on}")

    # WHERE
    where_conditions = []
    if since:
        where_conditions.append(f"{main_alias}.operation_time >= '{since}'")
    if until:
        where_conditions.append(f"{main_alias}.operation_time <= '{until}'")

    def replace_with_alias(expr):
        if isinstance(expr, str) and "." in expr:
            tbl, col = expr.split(".")
            alias = alias_map.get(tbl, tbl)
            return f"{alias}.{col}"
        return repr(expr)

    if filters:
        for key, val in filters.items():
            if key == "comparison":
                left_expr, op, right_expr = val
                left_expr = replace_with_alias(left_expr)
                right_expr = replace_with_alias(right_expr)
                where_conditions.append(f"{left_expr} {op} {right_expr}")
            else:
                tbl_name, col_name = key.split(".") if "." in key else (table, key)
                alias = alias_map.get(tbl_name, main_alias)
                where_conditions.append(f"{alias}.{col_name} = {repr(val)}")

    if where_conditions:
        sql_parts.append("WHERE " + " AND ".join(where_conditions))

    # GROUP BY
    # if aggregate:
    #     group_by_cols = [f"{alias_map[col.split('.')[0]]}.{col.split('.')[1]}" for col in returns]
    #     sql_parts.append("GROUP BY " + ", ".join(group_by_cols))

    # ORDER BY
    if order:
        sql_parts.append("ORDER BY " + ", ".join(f"{alias_map[col.split('.')[0]]}.{col.split('.')[1]}" for col in order))

    return "\n".join(sql_parts) + ";"


def run_provenance_query(query_dict, db_path="example.db"):
    sql = build_provenance_query(query_dict)
    print("Running SQL:\n", sql)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows

query = {
    "table": "price_audit",
    "since": "2022-01-01",
    "return": ["price_audit.old_price", "price_audit.new_price", "price_audit.operation_time"],
    "filter": {
        "comparison": ["price_audit.new_price", "<", "price_audit.old_price"],
        "product_id": 765
    },
    "order": ["price_audit.operation_time"]
}

results = run_provenance_query(query)
for row in results:
    print(row)

