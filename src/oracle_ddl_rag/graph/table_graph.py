"""使用 NetworkX 的圖形化資料表關聯導航。"""

from typing import Optional
from dataclasses import dataclass
import networkx as nx


@dataclass
class JoinStep:
    """JOIN 路徑中的單一步驟。"""
    step_number: int
    from_table: str
    to_table: str
    join_condition: str
    constraint_name: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "step": self.step_number,
            "from_table": self.from_table,
            "to_table": self.to_table,
            "join_condition": self.join_condition,
            "constraint_name": self.constraint_name,
        }


@dataclass
class JoinPath:
    """兩個資料表之間的完整 JOIN 路徑。"""
    source: str
    target: str
    steps: list[JoinStep]
    total_hops: int

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "total_hops": self.total_hops,
            "steps": [s.to_dict() for s in self.steps],
        }

    def to_sql(self) -> str:
        """產生 SQL JOIN 子句。"""
        if not self.steps:
            return ""

        joins = []
        for step in self.steps:
            joins.append(f"JOIN {step.to_table} ON {step.join_condition}")
        return "\n".join(joins)


class TableGraph:
    """透過外鍵約束導航資料表關聯的圖形。"""

    def __init__(self):
        """初始化空圖形。"""
        # 無向圖（JOIN 雙向運作）
        self.graph = nx.Graph()

    def add_relationship(
        self,
        parent_table: str,
        child_table: str,
        parent_columns: list[str],
        child_columns: list[str],
        constraint_name: Optional[str] = None,
    ) -> None:
        """將外鍵關聯加入圖形。

        參數：
            parent_table: 父（被參照）資料表名稱。
            child_table: 子（參照）資料表名稱。
            parent_columns: 外鍵中的父資料表欄位。
            child_columns: 外鍵中的子資料表欄位。
            constraint_name: 選用的外鍵約束名稱。
        """
        parent_table = parent_table.upper()
        child_table = child_table.upper()

        # 儲存雙向及欄位資訊
        self.graph.add_edge(
            parent_table,
            child_table,
            parent_columns=parent_columns,
            child_columns=child_columns,
            constraint_name=constraint_name,
            parent=parent_table,
            child=child_table,
        )

    def build_from_relationships(self, relationships: list[dict]) -> None:
        """從關聯字典列表建立圖形。

        參數：
            relationships: 包含 parent_table、child_table、
                          parent_columns、child_columns、constraint_name 的字典列表。
        """
        for rel in relationships:
            self.add_relationship(
                parent_table=rel["parent_table"],
                child_table=rel["child_table"],
                parent_columns=rel.get("parent_columns", []),
                child_columns=rel.get("child_columns", []),
                constraint_name=rel.get("constraint_name"),
            )

    def find_shortest_path(
        self,
        source: str,
        target: str,
        max_hops: int = 4,
    ) -> Optional[JoinPath]:
        """尋找兩個資料表之間的最短 JOIN 路徑。

        參數：
            source: 起始資料表名稱。
            target: 目標資料表名稱。
            max_hops: 最大中繼資料表數。

        回傳：
            JoinPath 物件，若無路徑則為 None。
        """
        source = source.upper()
        target = target.upper()

        if source not in self.graph:
            return None
        if target not in self.graph:
            return None

        try:
            path = nx.shortest_path(self.graph, source, target)
        except nx.NetworkXNoPath:
            return None

        # 檢查最大跳數（路徑長度 - 1 = 邊數）
        if len(path) - 1 > max_hops:
            return None

        # 建立 JOIN 步驟
        steps = []
        for i in range(len(path) - 1):
            from_table = path[i]
            to_table = path[i + 1]

            edge_data = self.graph.edges[from_table, to_table]
            join_condition = self._format_join_condition(
                from_table, to_table, edge_data
            )

            steps.append(JoinStep(
                step_number=i + 1,
                from_table=from_table,
                to_table=to_table,
                join_condition=join_condition,
                constraint_name=edge_data.get("constraint_name"),
            ))

        return JoinPath(
            source=source,
            target=target,
            steps=steps,
            total_hops=len(steps),
        )

    def _format_join_condition(
        self,
        from_table: str,
        to_table: str,
        edge_data: dict,
    ) -> str:
        """格式化兩個資料表之間的 JOIN 條件。

        參數：
            from_table: JOIN 中的第一個資料表。
            to_table: JOIN 中的第二個資料表。
            edge_data: 含有欄位對應的邊中繼資料。

        回傳：
            SQL JOIN 條件字串。
        """
        parent = edge_data.get("parent")
        child = edge_data.get("child")
        parent_cols = edge_data.get("parent_columns", [])
        child_cols = edge_data.get("child_columns", [])

        conditions = []
        for pc, cc in zip(parent_cols, child_cols):
            if from_table == parent:
                conditions.append(f"{parent}.{pc} = {child}.{cc}")
            else:
                conditions.append(f"{child}.{cc} = {parent}.{pc}")

        return " AND ".join(conditions) if conditions else f"{from_table}.id = {to_table}.id"

    def get_direct_relationship(
        self,
        table_a: str,
        table_b: str,
    ) -> Optional[dict]:
        """取得兩個資料表之間的直接外鍵關聯。

        參數：
            table_a: 第一個資料表名稱。
            table_b: 第二個資料表名稱。

        回傳：
            關聯字典，若未直接連接則為 None。
        """
        table_a = table_a.upper()
        table_b = table_b.upper()

        if not self.graph.has_edge(table_a, table_b):
            return None

        edge_data = self.graph.edges[table_a, table_b]
        parent = edge_data.get("parent")
        child = edge_data.get("child")

        return {
            "parent_table": parent,
            "child_table": child,
            "parent_columns": edge_data.get("parent_columns", []),
            "child_columns": edge_data.get("child_columns", []),
            "constraint_name": edge_data.get("constraint_name"),
            "join_condition": self._format_join_condition(
                table_a, table_b, edge_data
            ),
        }

    def get_related_tables(
        self,
        table_name: str,
        max_hops: int = 2,
    ) -> list[dict]:
        """取得指定資料表 N 跳內的所有資料表。

        參數：
            table_name: 起始資料表名稱。
            max_hops: 最大距離。

        回傳：
            包含 table_name 和 distance 的字典列表。
        """
        table_name = table_name.upper()

        if table_name not in self.graph:
            return []

        related = []
        lengths = nx.single_source_shortest_path_length(
            self.graph, table_name, cutoff=max_hops
        )

        for other_table, distance in lengths.items():
            if other_table != table_name:
                related.append({
                    "table_name": other_table,
                    "distance": distance,
                })

        # 依距離排序
        related.sort(key=lambda x: (x["distance"], x["table_name"]))
        return related

    def get_all_tables(self) -> list[str]:
        """取得圖形中所有資料表名稱。"""
        return list(self.graph.nodes())

    def get_stats(self) -> dict:
        """取得圖形統計資訊。"""
        return {
            "tables": self.graph.number_of_nodes(),
            "relationships": self.graph.number_of_edges(),
        }
