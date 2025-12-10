#!/usr/bin/env python
"""
離線結構注入腳本。

此腳本從 Oracle 資料庫提取 DDL 中繼資料，並填充 MCP 伺服器的
ChromaDB 向量儲存和 SQLite 快取。

憑證在執行時提示，永遠不會儲存或暴露給 AI 助手。

使用方式：
    uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user scott
"""

import argparse
import getpass
import sys
from pathlib import Path

# 將 src 加入路徑以供匯入
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import oracledb

from oracle_ddl_rag.extractors import DDLExtractor, RelationshipExtractor, EnumExtractor
from oracle_ddl_rag.storage import ChromaStore, SQLiteCache
from oracle_ddl_rag.embeddings import get_embedding_service
from oracle_ddl_rag.config import DATA_DIR


def main():
    parser = argparse.ArgumentParser(
        description="為 DDL RAG MCP 伺服器注入 Oracle 結構中繼資料"
    )
    parser.add_argument(
        "--dsn",
        required=True,
        help="Oracle DSN（例如：localhost:1521/ORCL 或 //host:port/service）",
    )
    parser.add_argument(
        "--user",
        required=True,
        help="Oracle 使用者名稱",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="注入前清除現有資料",
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="跳過嵌入產生（用於測試）",
    )
    args = parser.parse_args()

    # 提示輸入密碼（安全輸入）
    print("=" * 60)
    print("Oracle DDL RAG - 結構注入")
    print("=" * 60)
    print(f"\n連線至：{args.dsn}")
    print(f"使用者名稱：{args.user}")
    print("\n注意：密碼不會儲存，AI 助手看不到。\n")

    password = getpass.getpass("Oracle 密碼：")

    # 連線至 Oracle
    print("\n連線至 Oracle...")
    try:
        connection = oracledb.connect(
            user=args.user,
            password=password,
            dsn=args.dsn,
        )
        print("連線成功！")
    except oracledb.Error as e:
        print(f"連線 Oracle 時發生錯誤：{e}")
        sys.exit(1)

    # 初始化儲存
    print("\n初始化儲存...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    chroma = ChromaStore()
    cache = SQLiteCache()

    if args.clear:
        print("清除現有資料...")
        chroma.clear_all()
        cache.clear_all()

    # 初始化嵌入服務
    if not args.skip_embeddings:
        print("\n初始化嵌入服務...")
        embedding_service = get_embedding_service()
        print(f"使用嵌入模型：{embedding_service.model_name}")
        print(f"嵌入維度：{embedding_service.dimensions}")

    # 提取資料表
    print("\n" + "=" * 60)
    print("提取資料表...")
    print("=" * 60)

    ddl_extractor = DDLExtractor(connection)
    tables = ddl_extractor.get_all_tables()
    print(f"找到 {len(tables)} 個資料表")

    # 處理資料表
    for i, table in enumerate(tables, 1):
        print(f"  [{i}/{len(tables)}] {table.name}", end="")

        # 儲存至 SQLite 快取
        cache.upsert_table(table.to_dict())

        # 產生並儲存嵌入
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

            # 也儲存欄位嵌入
            for col in table.columns:
                col_doc = f"資料表 {table.name} 中的欄位 {col.name}：{col.data_type}"
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

        print(" [完成]")

    # 提取關聯
    print("\n" + "=" * 60)
    print("提取關聯...")
    print("=" * 60)

    rel_extractor = RelationshipExtractor(connection)
    relationships = rel_extractor.get_all_relationships()
    print(f"找到 {len(relationships)} 個外鍵關聯")

    for i, rel in enumerate(relationships, 1):
        print(f"  [{i}/{len(relationships)}] {rel.child_table} -> {rel.parent_table}", end="")

        # 儲存至 SQLite 快取
        cache.upsert_relationship(rel.to_dict())

        # 產生並儲存嵌入
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

        print(" [完成]")

    # 提取列舉值
    print("\n" + "=" * 60)
    print("提取列舉值...")
    print("=" * 60)

    enum_extractor = EnumExtractor(connection)
    enums = enum_extractor.extract_all()
    print(f"找到 {len(enums)} 個列舉定義")

    for i, enum in enumerate(enums, 1):
        print(f"  [{i}/{len(enums)}] {enum.table_name}.{enum.column_name}", end="")
        print(f" ({len(enum.values)} 個值，來源：{enum.source})", end="")

        # 儲存至 SQLite 快取
        cache.upsert_enum(enum.to_dict())

        print(" [完成]")

    # 更新同步時間戳記
    cache.update_last_sync_time()

    # 關閉連線
    connection.close()

    # 列印摘要
    print("\n" + "=" * 60)
    print("注入完成！")
    print("=" * 60)

    stats = cache.get_stats()
    chroma_stats = chroma.get_stats() if not args.skip_embeddings else {}

    print(f"\nSQLite 快取：")
    print(f"  - 資料表：{stats['tables']}")
    print(f"  - 列舉：{stats['enums']}")
    print(f"  - 關聯：{stats['relationships']}")

    if chroma_stats:
        print(f"\nChromaDB 向量：")
        print(f"  - 資料表嵌入：{chroma_stats.get('tables', 0)}")
        print(f"  - 欄位嵌入：{chroma_stats.get('columns', 0)}")
        print(f"  - 關聯嵌入：{chroma_stats.get('relationships', 0)}")

    print(f"\n資料儲存於：{DATA_DIR}")
    print("\n您現在可以使用以下指令啟動 MCP 伺服器：uv run oracle-ddl-mcp")


if __name__ == "__main__":
    main()
