"""Get detailed schema for a specific table."""

from ..storage import SQLiteCache


async def get_table_schema(
    table_name: str,
    include_indexes: bool = False,
) -> dict:
    """Get complete schema for a specific table by name.

    Use this tool to get detailed column information before writing SQL
    that references specific columns.

    Args:
        table_name: Exact table name (case-insensitive).
        include_indexes: Whether to include index definitions.

    Returns:
        Dictionary with complete table schema including columns,
        primary key, and optionally indexes.
    """
    cache = SQLiteCache()
    table = cache.get_table(table_name)

    if not table:
        # Try to suggest similar tables
        all_tables = cache.get_all_tables()
        suggestions = [
            t["table_name"] for t in all_tables
            if table_name.upper() in t["table_name"]
        ][:5]

        return {
            "success": False,
            "error": f"Table '{table_name}' not found.",
            "suggestions": suggestions if suggestions else None,
        }

    # Format columns for readability
    columns_formatted = []
    for col in table["columns"]:
        col_info = {
            "name": col["name"],
            "type": col["data_type"],
            "nullable": col["nullable"],
        }
        if col.get("comment"):
            col_info["comment"] = col["comment"]
        if col.get("data_default"):
            col_info["default"] = col["data_default"]
        columns_formatted.append(col_info)

    result = {
        "success": True,
        "table_name": table["table_name"],
        "description": table.get("comment") or "No description available",
        "row_count": table.get("row_count"),
        "primary_key": table.get("primary_key", []),
        "columns": columns_formatted,
    }

    if include_indexes and table.get("indexes"):
        result["indexes"] = table["indexes"]

    # Get relationships for this table
    relationships = cache.get_table_relationships(table_name)
    if relationships:
        result["relationships"] = [
            {
                "type": "parent" if r["parent_table"] == table["table_name"] else "child",
                "related_table": r["child_table"] if r["parent_table"] == table["table_name"] else r["parent_table"],
                "columns": r["child_columns"] if r["parent_table"] == table["table_name"] else r["parent_columns"],
            }
            for r in relationships
        ]

    return result
