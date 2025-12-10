"""Search database schemas by natural language query."""

from ..storage import ChromaStore
from ..embeddings import get_embedding_service
from ..config import DEFAULT_SEARCH_LIMIT


async def search_db_schema(
    query: str,
    limit: int = DEFAULT_SEARCH_LIMIT,
) -> dict:
    """Search database schemas by business concept or table name.

    This tool should be used BEFORE writing any SQL to find relevant tables.

    Args:
        query: Natural language description (e.g., "customer orders", "user authentication").
        limit: Maximum number of results (default: 10).

    Returns:
        Dictionary with matching tables and their relevance scores.
    """
    # Get embedding for the query
    embedding_service = get_embedding_service()
    query_embedding = embedding_service.embed_single(query)

    # Search in ChromaDB
    store = ChromaStore()
    results = store.search_tables(query_embedding, limit=limit)

    if not results:
        return {
            "success": True,
            "query": query,
            "results": [],
            "message": f"No tables found matching '{query}'. Try different keywords.",
        }

    # Format results
    formatted_results = []
    for r in results:
        metadata = r.get("metadata", {})
        formatted_results.append({
            "table_name": r["id"],
            "description": r.get("document", ""),
            "similarity_score": round(r.get("similarity", 0), 3),
            "column_count": metadata.get("column_count"),
            "has_comment": metadata.get("has_comment", False),
        })

    return {
        "success": True,
        "query": query,
        "result_count": len(formatted_results),
        "results": formatted_results,
    }
