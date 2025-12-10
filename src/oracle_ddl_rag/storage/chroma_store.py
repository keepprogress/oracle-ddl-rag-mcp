"""ChromaDB vector store for semantic search over database schemas."""

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
    """Vector database interface for schema semantic search."""

    def __init__(self, path: Optional[str] = None):
        """Initialize ChromaDB with persistent storage.

        Args:
            path: Override path for ChromaDB storage. Uses default if None.
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

        # Lazy-loaded collections
        self._tables: Optional[chromadb.Collection] = None
        self._columns: Optional[chromadb.Collection] = None
        self._relationships: Optional[chromadb.Collection] = None

    @property
    def tables(self) -> chromadb.Collection:
        """Get or create the tables collection."""
        if self._tables is None:
            self._tables = self._client.get_or_create_collection(
                name=COLLECTION_TABLES,
                metadata={"hnsw:space": "cosine"}
            )
        return self._tables

    @property
    def columns(self) -> chromadb.Collection:
        """Get or create the columns collection."""
        if self._columns is None:
            self._columns = self._client.get_or_create_collection(
                name=COLLECTION_COLUMNS,
                metadata={"hnsw:space": "cosine"}
            )
        return self._columns

    @property
    def relationships(self) -> chromadb.Collection:
        """Get or create the relationships collection."""
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
        """Search tables by semantic similarity.

        Args:
            query_embedding: Vector embedding of the search query.
            limit: Maximum number of results.

        Returns:
            List of matching tables with metadata and similarity scores.
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
        """Search columns by semantic similarity.

        Args:
            query_embedding: Vector embedding of the search query.
            limit: Maximum number of results.
            data_type: Optional filter by Oracle data type.

        Returns:
            List of matching columns with table names and metadata.
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
        """Search relationships by semantic similarity.

        Args:
            query_embedding: Vector embedding of the search query.
            limit: Maximum number of results.

        Returns:
            List of matching FK relationships.
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
        """Insert or update a table document.

        Args:
            table_id: Unique identifier (e.g., "ORDERS").
            document: Natural language description for embedding.
            metadata: Structured metadata (column_count, has_comment, etc.).
            embedding: Pre-computed vector embedding.
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
        """Insert or update a column document.

        Args:
            column_id: Unique identifier (e.g., "ORDERS.STATUS").
            document: Natural language description for embedding.
            metadata: Structured metadata (table_name, data_type, etc.).
            embedding: Pre-computed vector embedding.
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
        """Insert or update a relationship document.

        Args:
            rel_id: Unique identifier (e.g., "ORDER_ITEMS->ORDERS").
            document: Natural language description for embedding.
            metadata: Structured metadata (parent_table, child_table, etc.).
            embedding: Pre-computed vector embedding.
        """
        self.relationships.upsert(
            ids=[rel_id],
            documents=[document],
            metadatas=[metadata],
            embeddings=[embedding]
        )

    def get_table(self, table_id: str) -> Optional[dict]:
        """Get a specific table by ID.

        Args:
            table_id: Table name (case-insensitive, will be uppercased).

        Returns:
            Table document with metadata, or None if not found.
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
        """Delete all collections and reset the store."""
        for name in [COLLECTION_TABLES, COLLECTION_COLUMNS, COLLECTION_RELATIONSHIPS]:
            try:
                self._client.delete_collection(name)
            except Exception:
                pass
        self._tables = None
        self._columns = None
        self._relationships = None

    def get_stats(self) -> dict:
        """Get statistics about stored data.

        Returns:
            Dictionary with counts for each collection.
        """
        return {
            "tables": self.tables.count(),
            "columns": self.columns.count(),
            "relationships": self.relationships.count(),
        }

    @staticmethod
    def _format_results(results: dict) -> list[dict]:
        """Format ChromaDB query results into a list of dicts."""
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
                # Convert distance to similarity score (1 - cosine distance)
                item["similarity"] = 1 - results["distances"][0][i]
            formatted.append(item)

        return formatted
