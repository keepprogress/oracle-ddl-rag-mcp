"""Find multi-hop join path between two tables."""

from ..storage import SQLiteCache
from ..graph import TableGraph
from ..config import DEFAULT_PATH_MAX_HOPS


# Cached graph instance
_graph: TableGraph | None = None


def _get_graph() -> TableGraph:
    """Get or initialize the table graph."""
    global _graph
    if _graph is None:
        cache = SQLiteCache()
        relationships = cache.get_all_relationships()

        _graph = TableGraph()
        _graph.build_from_relationships(relationships)

    return _graph


async def find_join_path(
    source_table: str,
    target_table: str,
    max_hops: int = DEFAULT_PATH_MAX_HOPS,
) -> dict:
    """Find the shortest join path between two tables.

    Use this tool when tables are not directly related by a foreign key
    but you need to JOIN them through intermediate tables.

    Args:
        source_table: Starting table name.
        target_table: Destination table name.
        max_hops: Maximum number of intermediate tables (default: 4).

    Returns:
        Dictionary with ordered join steps and complete SQL.
    """
    graph = _get_graph()
    cache = SQLiteCache()

    # Check if tables exist
    source_data = cache.get_table(source_table)
    target_data = cache.get_table(target_table)

    if not source_data:
        return {
            "success": False,
            "error": f"Table '{source_table}' not found.",
        }
    if not target_data:
        return {
            "success": False,
            "error": f"Table '{target_table}' not found.",
        }

    # Find path
    path = graph.find_shortest_path(source_table, target_table, max_hops)

    if path is None:
        # Get related tables to suggest alternatives
        related_source = graph.get_related_tables(source_table.upper(), max_hops=2)
        related_target = graph.get_related_tables(target_table.upper(), max_hops=2)

        return {
            "success": False,
            "message": f"No join path found between '{source_table.upper()}' and '{target_table.upper()}' within {max_hops} hops.",
            "source_related_tables": [r["table_name"] for r in related_source[:5]],
            "target_related_tables": [r["table_name"] for r in related_target[:5]],
            "suggestions": [
                "Try increasing max_hops if the tables are distantly related",
                "The tables may not have FK relationships connecting them",
                "Check if there's a common table both are related to",
            ],
        }

    # Build complete SQL example
    sql_joins = []
    prev_table = source_table.upper()
    for step in path.steps:
        sql_joins.append(f"JOIN {step.to_table} ON {step.join_condition}")

    sql_example = f"SELECT *\nFROM {source_table.upper()}\n" + "\n".join(sql_joins)

    return {
        "success": True,
        "source": path.source,
        "target": path.target,
        "total_hops": path.total_hops,
        "path": path.to_dict()["steps"],
        "sql_example": sql_example,
    }
