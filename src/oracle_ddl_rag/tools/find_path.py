"""尋找兩個資料表之間的多跳 JOIN 路徑。"""

from ..storage import SQLiteCache
from ..graph import TableGraph
from ..config import DEFAULT_PATH_MAX_HOPS


# 快取的圖形實例
_graph: TableGraph | None = None


def _get_graph() -> TableGraph:
    """取得或初始化資料表圖形。"""
    global _graph
    if _graph is None:
        cache = SQLiteCache()
        relationships = cache.get_all_relationships()

        _graph = TableGraph()
        _graph.build_from_relationships(relationships)

    return _graph


async def find_join_path(
    source_table: str,
    target_table: str,
    max_hops: int = DEFAULT_PATH_MAX_HOPS,
) -> dict:
    """尋找兩個資料表之間的最短 JOIN 路徑。

    當資料表沒有直接外鍵關聯，但需要透過中繼資料表進行 JOIN 時使用此工具。

    參數：
        source_table: 起始資料表名稱。
        target_table: 目標資料表名稱。
        max_hops: 最大中繼資料表數（預設：4）。

    回傳：
        包含有序 JOIN 步驟及完整 SQL 的字典。
    """
    graph = _get_graph()
    cache = SQLiteCache()

    # 檢查資料表是否存在
    source_data = cache.get_table(source_table)
    target_data = cache.get_table(target_table)

    if not source_data:
        return {
            "success": False,
            "error": f"找不到資料表「{source_table}」。",
        }
    if not target_data:
        return {
            "success": False,
            "error": f"找不到資料表「{target_table}」。",
        }

    # 尋找路徑
    path = graph.find_shortest_path(source_table, target_table, max_hops)

    if path is None:
        # 取得相關資料表以建議替代方案
        related_source = graph.get_related_tables(source_table.upper(), max_hops=2)
        related_target = graph.get_related_tables(target_table.upper(), max_hops=2)

        return {
            "success": False,
            "message": f"在 {max_hops} 跳內找不到「{source_table.upper()}」和「{target_table.upper()}」之間的 JOIN 路徑。",
            "source_related_tables": [r["table_name"] for r in related_source[:5]],
            "target_related_tables": [r["table_name"] for r in related_target[:5]],
            "suggestions": [
                "如果資料表距離較遠，請嘗試增加 max_hops",
                "這些資料表可能沒有連接它們的外鍵關聯",
                "檢查是否有兩者都關聯的共同資料表",
            ],
        }

    # 建立完整的 SQL 範例
    sql_joins = []
    prev_table = source_table.upper()
    for step in path.steps:
        sql_joins.append(f"JOIN {step.to_table} ON {step.join_condition}")

    sql_example = f"SELECT *\nFROM {source_table.upper()}\n" + "\n".join(sql_joins)

    return {
        "success": True,
        "source": path.source,
        "target": path.target,
        "total_hops": path.total_hops,
        "path": path.to_dict()["steps"],
        "sql_example": sql_example,
    }
