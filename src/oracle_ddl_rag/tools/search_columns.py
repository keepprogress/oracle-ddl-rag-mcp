"""Search for columns across all tables."""

from typing import Optional

from ..storage import ChromaStore
from ..embeddings import get_embedding_service
from ..config import DEFAULT_SEARCH_LIMIT


async def search_columns(
    query: str,
    data_type: Optional[str] = None,
    limit: int = DEFAULT_SEARCH_LIMIT * 2,
) -> dict:
    """Search for columns across all tables by name or description.

    Use this tool when looking for a specific type of data field
    without knowing which table contains it.

    Args:
        query: Column name pattern or description (e.g., "email", "created date").
        data_type: Optional filter by Oracle data type (e.g., "VARCHAR2", "NUMBER", "DATE").
        limit: Maximum number of results (default: 20).

    Returns:
        Dictionary with matching columns and their table names.
    """
    # Get embedding for the query
    embedding_service = get_embedding_service()
    query_embedding = embedding_service.embed_single(query)

    # Search in ChromaDB
    store = ChromaStore()
    results = store.search_columns(
        query_embedding,
        limit=limit,
        data_type=data_type,
    )

    if not results:
        message = f"No columns found matching '{query}'"
        if data_type:
            message += f" with type '{data_type}'"
        message += ". Try different keywords or remove the type filter."

        return {
            "success": True,
            "query": query,
            "data_type_filter": data_type,
            "results": [],
            "message": message,
        }

    # Format results
    formatted_results = []
    for r in results:
        metadata = r.get("metadata", {})
        formatted_results.append({
            "table_name": metadata.get("table_name"),
            "column_name": metadata.get("column_name"),
            "data_type": metadata.get("data_type"),
            "description": r.get("document", ""),
            "similarity_score": round(r.get("similarity", 0), 3),
        })

    return {
        "success": True,
        "query": query,
        "data_type_filter": data_type,
        "result_count": len(formatted_results),
        "results": formatted_results,
    }
