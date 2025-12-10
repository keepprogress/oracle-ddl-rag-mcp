"""取得列舉型欄位（STATUS、TYPE 等）的有效值。"""

from ..storage import SQLiteCache


async def get_enum_values(
    table_name: str,
    column_name: str,
) -> dict:
    """取得 STATUS/TYPE 欄位的有效值。

    重要：在依狀態、類型或任何有限有效值的欄位篩選之前，請務必使用此工具。

    參數：
        table_name: 包含該欄位的資料表。
        column_name: 要取得值的欄位名稱。

    回傳：
        包含有效值及其含義（如有）的字典。
    """
    cache = SQLiteCache()

    # 首先檢查是否有定義列舉值
    enum = cache.get_enum(table_name, column_name)

    if enum:
        return {
            "success": True,
            "table_name": enum["table_name"],
            "column_name": enum["column_name"],
            "source": enum["source"],
            "values": enum["values"],
            "usage_hint": f"在依 {table_name}.{column_name} 篩選時，請使用這些精確值",
        }

    # 檢查資料表是否存在
    table = cache.get_table(table_name)
    if not table:
        return {
            "success": False,
            "error": f"找不到資料表「{table_name}」。",
        }

    # 檢查欄位是否存在
    column = None
    for col in table["columns"]:
        if col["name"].upper() == column_name.upper():
            column = col
            break

    if not column:
        available_columns = [c["name"] for c in table["columns"]]
        return {
            "success": False,
            "error": f"資料表「{table_name}」中找不到欄位「{column_name}」。",
            "available_columns": available_columns[:20],  # 限制顯示數量
        }

    # 欄位存在但沒有定義列舉值
    return {
        "success": False,
        "table_name": table_name.upper(),
        "column_name": column_name.upper(),
        "column_type": column["data_type"],
        "message": "此欄位沒有定義列舉值。",
        "suggestions": [
            "檢查 DDL 中是否有 CHECK 約束",
            "查看欄位定義中的註解",
            "這可能不是列舉欄位 - 值可能是自由格式",
            "您可能需要查詢資料表來找出不同的值",
        ],
    }
