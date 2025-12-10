"""取得特定資料表的詳細結構。"""

from ..storage import SQLiteCache


async def get_table_schema(
    table_name: str,
    include_indexes: bool = False,
) -> dict:
    """依名稱取得特定資料表的完整結構。

    在撰寫引用特定欄位的 SQL 前使用此工具取得詳細欄位資訊。

    參數：
        table_name: 精確的資料表名稱（不分大小寫）。
        include_indexes: 是否包含索引定義。

    回傳：
        包含完整資料表結構的字典，含欄位、主鍵及選用的索引。
    """
    cache = SQLiteCache()
    table = cache.get_table(table_name)

    if not table:
        # 嘗試建議類似的資料表
        all_tables = cache.get_all_tables()
        suggestions = [
            t["table_name"] for t in all_tables
            if table_name.upper() in t["table_name"]
        ][:5]

        return {
            "success": False,
            "error": f"找不到資料表「{table_name}」。",
            "suggestions": suggestions if suggestions else None,
        }

    # 格式化欄位以提高可讀性
    columns_formatted = []
    for col in table["columns"]:
        col_info = {
            "name": col["name"],
            "type": col["data_type"],
            "nullable": col["nullable"],
        }
        if col.get("comment"):
            col_info["comment"] = col["comment"]
        if col.get("data_default"):
            col_info["default"] = col["data_default"]
        columns_formatted.append(col_info)

    result = {
        "success": True,
        "table_name": table["table_name"],
        "description": table.get("comment") or "無可用描述",
        "row_count": table.get("row_count"),
        "primary_key": table.get("primary_key", []),
        "columns": columns_formatted,
    }

    if include_indexes and table.get("indexes"):
        result["indexes"] = table["indexes"]

    # 取得此資料表的關聯
    relationships = cache.get_table_relationships(table_name)
    if relationships:
        result["relationships"] = [
            {
                "type": "parent" if r["parent_table"] == table["table_name"] else "child",
                "related_table": r["child_table"] if r["parent_table"] == table["table_name"] else r["parent_table"],
                "columns": r["child_columns"] if r["parent_table"] == table["table_name"] else r["parent_columns"],
            }
            for r in relationships
        ]

    return result
