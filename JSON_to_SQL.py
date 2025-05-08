def build_provenance_query(params):
    table = params.get("table")
    # entity_id = params.get("entity_id")
    since = params.get("since")
    until = params.get("until")
    returns = params.get("return", [])
    filters = params.get("filter", {})
    joins = params.get("join", [])
    order = params.get("order", [])

    sql_parts = []
    alias_map = {}  # Map table name to alias (t1, t2, ...)

    # Assign aliases
    all_tables = [table] + [j["TABLE"] for j in joins]
    for i, tbl in enumerate(all_tables):
        alias_map[tbl] = f"t{i+1}"

    main_alias = alias_map[table]

    # SELECT clause
    select_clause = []
    if aggregate:
        func = aggregate["FUNCTION"]
        col = aggregate["ON"]
        agg_alias = aggregate["AS"]
        select_clause.append(f"{func}({alias_map[col.split('.')[0]]}.{col.split('.')[1]}) AS {agg_alias}")

    for col in returns:
        tbl_name, col_name = col.split(".")
        select_clause.append(f"{alias_map[tbl_name]}.{col_name}")

    sql_parts.append("SELECT " + ", ".join(select_clause))

    # FROM and JOIN clauses
    from_clause = f"FROM {table} {main_alias}"
    join_clauses = []
    for i, join in enumerate(joins):
        join_tbl = join["TABLE"]
        join_alias = alias_map[join_tbl]
        join_on = join["ON"]

        # Replace table names in join condition with aliases
        for tbl in alias_map:
            join_on = join_on.replace(f"{tbl}.", f"{alias_map[tbl]}.")

        join_clauses.append(f"JOIN {join_tbl} {join_alias} ON {join_on}")

    sql_parts.append(from_clause)
    sql_parts.extend(join_clauses)

    # WHERE clause
    # where_conditions = [f"{main_alias}.{table}_id = {entity_id}"]
    where_conditions = []

    if since:
        where_conditions.append(f"{main_alias}.operation_time >= TO_TIMESTAMP('{since}', 'YYYY-MM-DD')")
    if until:
        where_conditions.append(f"{main_alias}.operation_time <= TO_TIMESTAMP('{until}', 'YYYY-MM-DD')")

    # Handle filters
    if filters:
        for key, val in filters.items():
            if key == "comparison":
                # Handle: ["left_expr", "operator", "right_expr"]
                left_expr, op, right_expr = val

                def replace_with_alias(expr):
                    if isinstance(expr, str) and "." in expr:
                        tbl, col = expr.split(".")
                        alias = alias_map.get(tbl, tbl)  # fallback to tbl if not in map
                        return f"{alias}.{col}"
                    return repr(expr)  # for numbers or strings

                left_expr = replace_with_alias(left_expr)
                right_expr = replace_with_alias(right_expr)

                where_conditions.append(f"{left_expr} {op} {right_expr}")

            else:
                tbl_name, col_name = key.split(".") if "." in key else (table, key)
                alias = alias_map.get(tbl_name, main_alias)
                where_conditions.append(f"{alias}.{col_name} = {repr(val)}")
            # if key == "comparison":
            #     # Handle: ["left_expr", "operator", "right_expr"]
            #     left_expr, op, right_expr = val

            #     def replace_with_alias(expr):
            #         if "." in expr:
            #             tbl, col = expr.split(".")
            #             alias = alias_map.get(tbl, tbl)  # fallback to tbl if not in map
            #             return f"{alias}.{col}"
            #         return expr  # raw literal (e.g., number)

            #     left_expr = replace_with_alias(left_expr)
            #     right_expr = replace_with_alias(right_expr)

            #     where_conditions.append(f"{left_expr} {op} {right_expr}")
            # else:
            #     tbl_name, col_name = key.split(".") if "." in key else (table, key)
            #     alias = alias_map.get(tbl_name, main_alias)
            #     where_conditions.append(f"{alias}.{col_name} = {val}")

    if where_conditions:
        sql_parts.append("WHERE " + " AND ".join(where_conditions))

    # GROUP BY (if aggregation used)
    if aggregate:
        group_by_cols = [f"{alias_map[col.split('.')[0]]}.{col.split('.')[1]}" for col in returns]
        sql_parts.append("GROUP BY " + ", ".join(group_by_cols))
    
    # ORDER BY (if present)
    if order:
        order_clause = []
        for col in order:
            tbl_name, col_name = col.split(".")
            order_clause.append(f"{alias_map[tbl_name]}.{col_name}")
        sql_parts.append("ORDER BY " + ", ".join(order_clause))

    sql_parts.append(";")
    return "\n".join(sql_parts)




# CGPA

query = {
  "table": "price_audit",
  "since": "2022-01-01",
  "return": ["price_audit.old_price", "price_audit.new_price", "price_audit.operation_time"],
  "filter": {
    "comparison": ["price_audit.new_price", "<", "price_audit.old_price"],
  "product_id" : 765
  }
}


# Price change
# query = {
#   "table": "order_audit",
#   "entity_id": 321,
#   "since": "2021-01-01",
#   "until": "2021-12-31",
#   "return": ["order_audit.old_status", "order_audit.new_status", "order_audit.operation_time"]
# }

print(build_provenance_query(query))
