"""用於資料庫結構語意搜尋的 ChromaDB 向量儲存。"""

from typing import Optional
import chromadb
from chromadb.config import Settings

from ..config import (
    CHROMA_PATH,
    COLLECTION_TABLES,
    COLLECTION_COLUMNS,
    COLLECTION_RELATIONSHIPS,
    MAX_SEARCH_LIMIT,
)


class ChromaStore:
    """用於結構語意搜尋的向量資料庫介面。"""

    def __init__(self, path: Optional[str] = None):
        """以持久化儲存初始化 ChromaDB。

        參數：
            path: ChromaDB 儲存的覆寫路徑。若為 None 則使用預設值。
        """
        db_path = path or str(CHROMA_PATH)
        CHROMA_PATH.mkdir(parents=True, exist_ok=True)

        self._client = chromadb.PersistentClient(
            path=db_path,
            settings=Settings(
                anonymized_telemetry=False,
                allow_reset=True,
            )
        )

        # 延遲載入的集合
        self._tables: Optional[chromadb.Collection] = None
        self._columns: Optional[chromadb.Collection] = None
        self._relationships: Optional[chromadb.Collection] = None

    @property
    def tables(self) -> chromadb.Collection:
        """取得或建立資料表集合。"""
        if self._tables is None:
            self._tables = self._client.get_or_create_collection(
                name=COLLECTION_TABLES,
                metadata={"hnsw:space": "cosine"}
            )
        return self._tables

    @property
    def columns(self) -> chromadb.Collection:
        """取得或建立欄位集合。"""
        if self._columns is None:
            self._columns = self._client.get_or_create_collection(
                name=COLLECTION_COLUMNS,
                metadata={"hnsw:space": "cosine"}
            )
        return self._columns

    @property
    def relationships(self) -> chromadb.Collection:
        """取得或建立關聯集合。"""
        if self._relationships is None:
            self._relationships = self._client.get_or_create_collection(
                name=COLLECTION_RELATIONSHIPS,
                metadata={"hnsw:space": "cosine"}
            )
        return self._relationships

    def search_tables(
        self,
        query_embedding: list[float],
        limit: int = 10,
    ) -> list[dict]:
        """依語意相似度搜尋資料表。

        參數：
            query_embedding: 搜尋查詢的向量嵌入。
            limit: 最大結果數。

        回傳：
            包含中繼資料和相似度分數的符合資料表列表。
        """
        limit = min(limit, MAX_SEARCH_LIMIT)
        results = self.tables.query(
            query_embeddings=[query_embedding],
            n_results=limit,
            include=["documents", "metadatas", "distances"]
        )
        return self._format_results(results)

    def search_columns(
        self,
        query_embedding: list[float],
        limit: int = 20,
        data_type: Optional[str] = None,
    ) -> list[dict]:
        """依語意相似度搜尋欄位。

        參數：
            query_embedding: 搜尋查詢的向量嵌入。
            limit: 最大結果數。
            data_type: 選用，依 Oracle 資料類型篩選。

        回傳：
            包含資料表名稱和中繼資料的符合欄位列表。
        """
        limit = min(limit, MAX_SEARCH_LIMIT)
        where_filter = {"data_type": data_type.upper()} if data_type else None

        results = self.columns.query(
            query_embeddings=[query_embedding],
            n_results=limit,
            where=where_filter,
            include=["documents", "metadatas", "distances"]
        )
        return self._format_results(results)

    def search_relationships(
        self,
        query_embedding: list[float],
        limit: int = 10,
    ) -> list[dict]:
        """依語意相似度搜尋關聯。

        參數：
            query_embedding: 搜尋查詢的向量嵌入。
            limit: 最大結果數。

        回傳：
            符合的外鍵關聯列表。
        """
        limit = min(limit, MAX_SEARCH_LIMIT)
        results = self.relationships.query(
            query_embeddings=[query_embedding],
            n_results=limit,
            include=["documents", "metadatas", "distances"]
        )
        return self._format_results(results)

    def upsert_table(
        self,
        table_id: str,
        document: str,
        metadata: dict,
        embedding: list[float],
    ) -> None:
        """插入或更新資料表文件。

        參數：
            table_id: 唯一識別碼（例如：「ORDERS」）。
            document: 用於嵌入的自然語言描述。
            metadata: 結構化中繼資料（column_count、has_comment 等）。
            embedding: 預先計算的向量嵌入。
        """
        self.tables.upsert(
            ids=[table_id],
            documents=[document],
            metadatas=[metadata],
            embeddings=[embedding]
        )

    def upsert_column(
        self,
        column_id: str,
        document: str,
        metadata: dict,
        embedding: list[float],
    ) -> None:
        """插入或更新欄位文件。

        參數：
            column_id: 唯一識別碼（例如：「ORDERS.STATUS」）。
            document: 用於嵌入的自然語言描述。
            metadata: 結構化中繼資料（table_name、data_type 等）。
            embedding: 預先計算的向量嵌入。
        """
        self.columns.upsert(
            ids=[column_id],
            documents=[document],
            metadatas=[metadata],
            embeddings=[embedding]
        )

    def upsert_relationship(
        self,
        rel_id: str,
        document: str,
        metadata: dict,
        embedding: list[float],
    ) -> None:
        """插入或更新關聯文件。

        參數：
            rel_id: 唯一識別碼（例如：「ORDER_ITEMS->ORDERS」）。
            document: 用於嵌入的自然語言描述。
            metadata: 結構化中繼資料（parent_table、child_table 等）。
            embedding: 預先計算的向量嵌入。
        """
        self.relationships.upsert(
            ids=[rel_id],
            documents=[document],
            metadatas=[metadata],
            embeddings=[embedding]
        )

    def get_table(self, table_id: str) -> Optional[dict]:
        """依 ID 取得特定資料表。

        參數：
            table_id: 資料表名稱（不分大小寫，將轉為大寫）。

        回傳：
            包含中繼資料的資料表文件，若找不到則為 None。
        """
        table_id = table_id.upper()
        results = self.tables.get(
            ids=[table_id],
            include=["documents", "metadatas"]
        )
        if results["ids"]:
            return {
                "id": results["ids"][0],
                "document": results["documents"][0],
                "metadata": results["metadatas"][0] if results["metadatas"] else {}
            }
        return None

    def clear_all(self) -> None:
        """刪除所有集合並重置儲存。"""
        for name in [COLLECTION_TABLES, COLLECTION_COLUMNS, COLLECTION_RELATIONSHIPS]:
            try:
                self._client.delete_collection(name)
            except Exception:
                pass
        self._tables = None
        self._columns = None
        self._relationships = None

    def get_stats(self) -> dict:
        """取得儲存資料的統計資訊。

        回傳：
            包含各集合計數的字典。
        """
        return {
            "tables": self.tables.count(),
            "columns": self.columns.count(),
            "relationships": self.relationships.count(),
        }

    @staticmethod
    def _format_results(results: dict) -> list[dict]:
        """將 ChromaDB 查詢結果格式化為字典列表。"""
        if not results["ids"] or not results["ids"][0]:
            return []

        formatted = []
        for i in range(len(results["ids"][0])):
            item = {
                "id": results["ids"][0][i],
                "document": results["documents"][0][i] if results.get("documents") else None,
                "metadata": results["metadatas"][0][i] if results.get("metadatas") else {},
            }
            if results.get("distances"):
                # 將距離轉換為相似度分數（1 - 餘弦距離）
                item["similarity"] = 1 - results["distances"][0][i]
            formatted.append(item)

        return formatted
