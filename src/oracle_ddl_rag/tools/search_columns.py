"""在所有資料表中搜尋欄位。"""

from typing import Optional

from ..storage import ChromaStore
from ..embeddings import get_embedding_service
from ..config import DEFAULT_SEARCH_LIMIT


async def search_columns(
    query: str,
    data_type: Optional[str] = None,
    limit: int = DEFAULT_SEARCH_LIMIT * 2,
) -> dict:
    """依名稱或描述在所有資料表中搜尋欄位。

    當尋找特定類型的資料欄位但不知道在哪個資料表時使用此工具。

    參數：
        query: 欄位名稱模式或描述（例如：「email」、「建立日期」）。
        data_type: 選用，依 Oracle 資料類型篩選（例如：「VARCHAR2」、「NUMBER」、「DATE」）。
        limit: 最大結果數（預設：20）。

    回傳：
        包含符合欄位及其資料表名稱的字典。
    """
    # 取得查詢的嵌入向量
    embedding_service = get_embedding_service()
    query_embedding = embedding_service.embed_single(query)

    # 在 ChromaDB 中搜尋
    store = ChromaStore()
    results = store.search_columns(
        query_embedding,
        limit=limit,
        data_type=data_type,
    )

    if not results:
        message = f"找不到符合「{query}」的欄位"
        if data_type:
            message += f"（類型為「{data_type}」）"
        message += "。請嘗試其他關鍵字或移除類型篩選。"

        return {
            "success": True,
            "query": query,
            "data_type_filter": data_type,
            "results": [],
            "message": message,
        }

    # 格式化結果
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
