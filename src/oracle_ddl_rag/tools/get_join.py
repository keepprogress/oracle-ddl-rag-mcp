"""取得兩個資料表之間正確的 JOIN 條件。"""

from ..storage import SQLiteCache
from ..graph import TableGraph


async def get_join_pattern(
    table_a: str,
    table_b: str,
) -> dict:
    """取得兩個資料表之間已驗證的 JOIN 條件。

    重要：在撰寫 JOIN 之前請務必使用此工具，以確保正確的欄位對應並避免幻覺的關聯。

    參數：
        table_a: 第一個資料表名稱。
        table_b: 第二個資料表名稱。

    回傳：
        包含 JOIN 條件及關聯詳情的字典。
    """
    cache = SQLiteCache()

    # 從快取取得直接關聯
    relationship = cache.get_relationship(table_a, table_b)

    if relationship:
        # 建立 JOIN 條件字串
        conditions = []
        for pc, cc in zip(relationship["parent_columns"], relationship["child_columns"]):
            conditions.append(
                f"{relationship['child_table']}.{cc} = {relationship['parent_table']}.{pc}"
            )
        join_condition = " AND ".join(conditions)

        return {
            "success": True,
            "relationship_type": "direct_fk",
            "parent_table": relationship["parent_table"],
            "child_table": relationship["child_table"],
            "join_condition": join_condition,
            "constraint_name": relationship.get("constraint_name"),
            "sql_example": f"""SELECT *
FROM {relationship['child_table']} c
JOIN {relationship['parent_table']} p ON {join_condition}""",
        }

    # 檢查資料表是否存在
    table_a_data = cache.get_table(table_a)
    table_b_data = cache.get_table(table_b)

    if not table_a_data:
        return {
            "success": False,
            "error": f"找不到資料表「{table_a}」。",
        }
    if not table_b_data:
        return {
            "success": False,
            "error": f"找不到資料表「{table_b}」。",
        }

    # 資料表存在但沒有直接外鍵 - 建議使用 find_join_path
    return {
        "success": False,
        "message": f"「{table_a.upper()}」和「{table_b.upper()}」之間沒有直接的外鍵關聯。",
        "suggestions": [
            "使用「find_join_path」工具尋找經過中繼資料表的間接路徑",
            "檢查是否有兩者都參照的共同查找資料表",
            "這些資料表可能透過業務邏輯相關，但沒有外鍵約束",
        ],
    }
