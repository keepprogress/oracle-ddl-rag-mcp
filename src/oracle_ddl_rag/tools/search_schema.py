"""依自然語言查詢搜尋資料庫結構。"""

from ..storage import ChromaStore
from ..embeddings import get_embedding_service
from ..config import DEFAULT_SEARCH_LIMIT


async def search_db_schema(
    query: str,
    limit: int = DEFAULT_SEARCH_LIMIT,
) -> dict:
    """依業務概念或資料表名稱搜尋資料庫結構。

    在撰寫任何 SQL 之前應使用此工具來尋找相關資料表。

    參數：
        query: 自然語言描述（例如：「客戶訂單」、「使用者驗證」）。
        limit: 最大結果數（預設：10）。

    回傳：
        包含符合資料表及相關分數的字典。
    """
    # 取得查詢的嵌入向量
    embedding_service = get_embedding_service()
    query_embedding = embedding_service.embed_single(query)

    # 在 ChromaDB 中搜尋
    store = ChromaStore()
    results = store.search_tables(query_embedding, limit=limit)

    if not results:
        return {
            "success": True,
            "query": query,
            "results": [],
            "message": f"找不到符合「{query}」的資料表。請嘗試其他關鍵字。",
        }

    # 格式化結果
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
