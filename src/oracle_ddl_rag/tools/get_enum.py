"""Get valid values for enum-like columns (STATUS, TYPE, etc.)."""

from ..storage import SQLiteCache


async def get_enum_values(
    table_name: str,
    column_name: str,
) -> dict:
    """Get valid values for a STATUS/TYPE column.

    IMPORTANT: Always use this tool BEFORE filtering by status, type,
    or any column that has a limited set of valid values.

    Args:
        table_name: Table containing the column.
        column_name: Column name to get values for.

    Returns:
        Dictionary with valid values and their meanings.
    """
    cache = SQLiteCache()

    # First check if enum values are defined
    enum = cache.get_enum(table_name, column_name)

    if enum:
        return {
            "success": True,
            "table_name": enum["table_name"],
            "column_name": enum["column_name"],
            "source": enum["source"],
            "values": enum["values"],
            "usage_hint": f"Use these exact values when filtering by {table_name}.{column_name}",
        }

    # Check if the table exists
    table = cache.get_table(table_name)
    if not table:
        return {
            "success": False,
            "error": f"Table '{table_name}' not found.",
        }

    # Check if the column exists
    column = None
    for col in table["columns"]:
        if col["name"].upper() == column_name.upper():
            column = col
            break

    if not column:
        available_columns = [c["name"] for c in table["columns"]]
        return {
            "success": False,
            "error": f"Column '{column_name}' not found in table '{table_name}'.",
            "available_columns": available_columns[:20],  # Limit for readability
        }

    # Column exists but no enum defined
    return {
        "success": False,
        "table_name": table_name.upper(),
        "column_name": column_name.upper(),
        "column_type": column["data_type"],
        "message": "No enum values are defined for this column.",
        "suggestions": [
            "Check if this column has a CHECK constraint in the DDL",
            "Look for comments in the column definition",
            "This may not be an enum column - values might be free-form",
            "You may need to query the table to find distinct values",
        ],
    }
