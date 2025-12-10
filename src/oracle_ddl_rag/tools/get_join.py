"""Get correct JOIN condition between two tables."""

from ..storage import SQLiteCache
from ..graph import TableGraph


async def get_join_pattern(
    table_a: str,
    table_b: str,
) -> dict:
    """Get the verified JOIN condition between two tables.

    IMPORTANT: Always use this tool BEFORE writing JOINs to ensure
    correct column mappings and avoid hallucinated relationships.

    Args:
        table_a: First table name.
        table_b: Second table name.

    Returns:
        Dictionary with JOIN condition and relationship details.
    """
    cache = SQLiteCache()

    # Get direct relationship from cache
    relationship = cache.get_relationship(table_a, table_b)

    if relationship:
        # Build JOIN condition string
        conditions = []
        for pc, cc in zip(relationship["parent_columns"], relationship["child_columns"]):
            conditions.append(
                f"{relationship['child_table']}.{cc} = {relationship['parent_table']}.{pc}"
            )
        join_condition = " AND ".join(conditions)

        return {
            "success": True,
            "relationship_type": "direct_fk",
            "parent_table": relationship["parent_table"],
            "child_table": relationship["child_table"],
            "join_condition": join_condition,
            "constraint_name": relationship.get("constraint_name"),
            "sql_example": f"""SELECT *
FROM {relationship['child_table']} c
JOIN {relationship['parent_table']} p ON {join_condition}""",
        }

    # Check if tables exist
    table_a_data = cache.get_table(table_a)
    table_b_data = cache.get_table(table_b)

    if not table_a_data:
        return {
            "success": False,
            "error": f"Table '{table_a}' not found.",
        }
    if not table_b_data:
        return {
            "success": False,
            "error": f"Table '{table_b}' not found.",
        }

    # Tables exist but no direct FK - suggest using find_join_path
    return {
        "success": False,
        "message": f"No direct foreign key relationship between '{table_a.upper()}' and '{table_b.upper()}'.",
        "suggestions": [
            "Use the 'find_join_path' tool to find an indirect path through intermediate tables",
            "Check if there's a common lookup table they both reference",
            "The tables may be related through business logic but not FK constraints",
        ],
    }
