#!/usr/bin/env python
"""
Offline schema ingestion script.

This script extracts DDL metadata from an Oracle database and populates
the ChromaDB vector store and SQLite cache for the MCP server.

Credentials are prompted at runtime and NEVER stored or exposed to AI assistants.

Usage:
    uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user scott
"""

import argparse
import getpass
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import oracledb

from oracle_ddl_rag.extractors import DDLExtractor, RelationshipExtractor, EnumExtractor
from oracle_ddl_rag.storage import ChromaStore, SQLiteCache
from oracle_ddl_rag.embeddings import get_embedding_service
from oracle_ddl_rag.config import DATA_DIR


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Oracle schema metadata for DDL RAG MCP Server"
    )
    parser.add_argument(
        "--dsn",
        required=True,
        help="Oracle DSN (e.g., localhost:1521/ORCL or //host:port/service)",
    )
    parser.add_argument(
        "--user",
        required=True,
        help="Oracle username",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing data before ingestion",
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Skip embedding generation (useful for testing)",
    )
    args = parser.parse_args()

    # Prompt for password (secure input)
    print("=" * 60)
    print("Oracle DDL RAG - Schema Ingestion")
    print("=" * 60)
    print(f"\nConnecting to: {args.dsn}")
    print(f"Username: {args.user}")
    print("\nNote: Password is not stored and not visible to AI assistants.\n")

    password = getpass.getpass("Oracle Password: ")

    # Connect to Oracle
    print("\nConnecting to Oracle...")
    try:
        connection = oracledb.connect(
            user=args.user,
            password=password,
            dsn=args.dsn,
        )
        print("Connected successfully!")
    except oracledb.Error as e:
        print(f"Error connecting to Oracle: {e}")
        sys.exit(1)

    # Initialize storage
    print("\nInitializing storage...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    chroma = ChromaStore()
    cache = SQLiteCache()

    if args.clear:
        print("Clearing existing data...")
        chroma.clear_all()
        cache.clear_all()

    # Initialize embedding service
    if not args.skip_embeddings:
        print("\nInitializing embedding service...")
        embedding_service = get_embedding_service()
        print(f"Using embedding model: {embedding_service.model_name}")
        print(f"Embedding dimensions: {embedding_service.dimensions}")

    # Extract tables
    print("\n" + "=" * 60)
    print("Extracting Tables...")
    print("=" * 60)

    ddl_extractor = DDLExtractor(connection)
    tables = ddl_extractor.get_all_tables()
    print(f"Found {len(tables)} tables")

    # Process tables
    for i, table in enumerate(tables, 1):
        print(f"  [{i}/{len(tables)}] {table.name}", end="")

        # Store in SQLite cache
        cache.upsert_table(table.to_dict())

        # Generate and store embeddings
        if not args.skip_embeddings:
            doc = table.to_document()
            embedding = embedding_service.embed_single(doc)

            chroma.upsert_table(
                table_id=table.name,
                document=doc,
                metadata={
                    "table_name": table.name,
                    "column_count": len(table.columns),
                    "has_comment": bool(table.comment),
                    "row_count": table.row_count,
                },
                embedding=embedding,
            )

            # Also store column embeddings
            for col in table.columns:
                col_doc = f"Column {col.name} in table {table.name}: {col.data_type}"
                if col.comment:
                    col_doc += f" - {col.comment}"

                col_embedding = embedding_service.embed_single(col_doc)
                chroma.upsert_column(
                    column_id=f"{table.name}.{col.name}",
                    document=col_doc,
                    metadata={
                        "table_name": table.name,
                        "column_name": col.name,
                        "data_type": col.data_type,
                        "nullable": col.nullable,
                    },
                    embedding=col_embedding,
                )

        print(" [OK]")

    # Extract relationships
    print("\n" + "=" * 60)
    print("Extracting Relationships...")
    print("=" * 60)

    rel_extractor = RelationshipExtractor(connection)
    relationships = rel_extractor.get_all_relationships()
    print(f"Found {len(relationships)} foreign key relationships")

    for i, rel in enumerate(relationships, 1):
        print(f"  [{i}/{len(relationships)}] {rel.child_table} -> {rel.parent_table}", end="")

        # Store in SQLite cache
        cache.upsert_relationship(rel.to_dict())

        # Generate and store embeddings
        if not args.skip_embeddings:
            doc = rel.to_document()
            embedding = embedding_service.embed_single(doc)

            chroma.upsert_relationship(
                rel_id=f"{rel.child_table}->{rel.parent_table}",
                document=doc,
                metadata={
                    "parent_table": rel.parent_table,
                    "child_table": rel.child_table,
                    "constraint_name": rel.constraint_name,
                },
                embedding=embedding,
            )

        print(" [OK]")

    # Extract enum values
    print("\n" + "=" * 60)
    print("Extracting Enum Values...")
    print("=" * 60)

    enum_extractor = EnumExtractor(connection)
    enums = enum_extractor.extract_all()
    print(f"Found {len(enums)} enum definitions")

    for i, enum in enumerate(enums, 1):
        print(f"  [{i}/{len(enums)}] {enum.table_name}.{enum.column_name}", end="")
        print(f" ({len(enum.values)} values, source: {enum.source})", end="")

        # Store in SQLite cache
        cache.upsert_enum(enum.to_dict())

        print(" [OK]")

    # Update sync timestamp
    cache.update_last_sync_time()

    # Close connection
    connection.close()

    # Print summary
    print("\n" + "=" * 60)
    print("Ingestion Complete!")
    print("=" * 60)

    stats = cache.get_stats()
    chroma_stats = chroma.get_stats() if not args.skip_embeddings else {}

    print(f"\nSQLite Cache:")
    print(f"  - Tables: {stats['tables']}")
    print(f"  - Enums: {stats['enums']}")
    print(f"  - Relationships: {stats['relationships']}")

    if chroma_stats:
        print(f"\nChromaDB Vectors:")
        print(f"  - Table embeddings: {chroma_stats.get('tables', 0)}")
        print(f"  - Column embeddings: {chroma_stats.get('columns', 0)}")
        print(f"  - Relationship embeddings: {chroma_stats.get('relationships', 0)}")

    print(f"\nData stored in: {DATA_DIR}")
    print("\nYou can now start the MCP server with: uv run oracle-ddl-mcp")


if __name__ == "__main__":
    main()
