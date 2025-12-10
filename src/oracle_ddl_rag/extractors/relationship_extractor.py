"""Oracle 外鍵關聯提取器。"""

from dataclasses import dataclass
import oracledb


@dataclass
class ForeignKeyInfo:
    """外鍵關聯中繼資料。"""
    constraint_name: str
    child_table: str
    child_columns: list[str]
    parent_table: str
    parent_columns: list[str]

    def to_dict(self) -> dict:
        return {
            "constraint_name": self.constraint_name,
            "child_table": self.child_table,
            "child_columns": self.child_columns,
            "parent_table": self.parent_table,
            "parent_columns": self.parent_columns,
        }

    def to_document(self) -> str:
        """建立用於嵌入的自然語言描述。"""
        child_cols = ", ".join(self.child_columns)
        parent_cols = ", ".join(self.parent_columns)

        join_condition = " AND ".join(
            f"{self.child_table}.{cc} = {self.parent_table}.{pc}"
            for cc, pc in zip(self.child_columns, self.parent_columns)
        )

        return f"""外鍵：{self.child_table} 參照 {self.parent_table}
約束：{self.constraint_name}
子欄位：{self.child_table}.{child_cols}
父欄位：{self.parent_table}.{parent_cols}
JOIN 條件：{join_condition}"""


class RelationshipExtractor:
    """從 Oracle 資料庫提取外鍵關聯。"""

    # 取得所有外鍵關聯及欄位對應
    FK_QUERY = """
        SELECT
            c.constraint_name,
            c.table_name as child_table,
            rc.table_name as parent_table
        FROM user_constraints c
        JOIN user_constraints rc ON c.r_constraint_name = rc.constraint_name
        WHERE c.constraint_type = 'R'
        ORDER BY c.table_name, rc.table_name
    """

    # 取得特定外鍵約束的欄位
    FK_COLUMNS_QUERY = """
        SELECT
            cc.column_name as child_column,
            rcc.column_name as parent_column
        FROM user_cons_columns cc
        JOIN user_constraints c ON cc.constraint_name = c.constraint_name
        JOIN user_constraints rc ON c.r_constraint_name = rc.constraint_name
        JOIN user_cons_columns rcc
            ON rc.constraint_name = rcc.constraint_name
            AND cc.position = rcc.position
        WHERE c.constraint_name = :constraint_name
        ORDER BY cc.position
    """

    def __init__(self, connection: oracledb.Connection):
        """以 Oracle 連線初始化。

        參數：
            connection: 活動的 Oracle 資料庫連線。
        """
        self._conn = connection

    def get_all_relationships(self) -> list[ForeignKeyInfo]:
        """提取所有外鍵關聯。

        回傳：
            ForeignKeyInfo 物件列表。
        """
        cursor = self._conn.cursor()
        cursor.execute(self.FK_QUERY)

        relationships = []
        for constraint_name, child_table, parent_table in cursor:
            child_cols, parent_cols = self._get_fk_columns(constraint_name)

            relationships.append(ForeignKeyInfo(
                constraint_name=constraint_name,
                child_table=child_table,
                child_columns=child_cols,
                parent_table=parent_table,
                parent_columns=parent_cols,
            ))

        cursor.close()
        return relationships

    def get_table_relationships(self, table_name: str) -> list[ForeignKeyInfo]:
        """取得涉及特定資料表的所有外鍵關聯。

        參數：
            table_name: 資料表名稱（不分大小寫）。

        回傳：
            該資料表作為父表或子表的 ForeignKeyInfo 列表。
        """
        table_name = table_name.upper()
        cursor = self._conn.cursor()

        cursor.execute("""
            SELECT
                c.constraint_name,
                c.table_name as child_table,
                rc.table_name as parent_table
            FROM user_constraints c
            JOIN user_constraints rc ON c.r_constraint_name = rc.constraint_name
            WHERE c.constraint_type = 'R'
            AND (c.table_name = :table_name OR rc.table_name = :table_name)
            ORDER BY c.table_name, rc.table_name
        """, {"table_name": table_name})

        relationships = []
        for constraint_name, child_table, parent_table in cursor:
            child_cols, parent_cols = self._get_fk_columns(constraint_name)

            relationships.append(ForeignKeyInfo(
                constraint_name=constraint_name,
                child_table=child_table,
                child_columns=child_cols,
                parent_table=parent_table,
                parent_columns=parent_cols,
            ))

        cursor.close()
        return relationships

    def _get_fk_columns(self, constraint_name: str) -> tuple[list[str], list[str]]:
        """取得外鍵約束的子欄位和父欄位。

        參數：
            constraint_name: 外鍵約束名稱。

        回傳：
            (子欄位, 父欄位) 的元組。
        """
        cursor = self._conn.cursor()
        cursor.execute(self.FK_COLUMNS_QUERY, {"constraint_name": constraint_name})

        child_cols = []
        parent_cols = []
        for child_col, parent_col in cursor:
            child_cols.append(child_col)
            parent_cols.append(parent_col)

        cursor.close()
        return child_cols, parent_cols
