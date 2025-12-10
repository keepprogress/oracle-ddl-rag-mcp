"""Oracle DDL 提取器，用於資料表、欄位和索引。"""

from typing import Optional
from dataclasses import dataclass
import oracledb


@dataclass
class ColumnInfo:
    """欄位中繼資料。"""
    name: str
    data_type: str
    nullable: bool
    data_default: Optional[str]
    comment: Optional[str]

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "data_default": self.data_default,
            "comment": self.comment,
        }


@dataclass
class IndexInfo:
    """索引中繼資料。"""
    name: str
    columns: list[str]
    is_unique: bool

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "columns": self.columns,
            "is_unique": self.is_unique,
        }


@dataclass
class TableInfo:
    """包含欄位的資料表中繼資料。"""
    name: str
    comment: Optional[str]
    row_count: Optional[int]
    columns: list[ColumnInfo]
    primary_key: list[str]
    indexes: list[IndexInfo]

    def to_dict(self) -> dict:
        return {
            "table_name": self.name,
            "comment": self.comment,
            "row_count": self.row_count,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key": self.primary_key,
            "indexes": [i.to_dict() for i in self.indexes],
        }

    def to_document(self) -> str:
        """建立用於嵌入的自然語言描述。"""
        col_descriptions = []
        for c in self.columns[:15]:  # 限制嵌入大小
            desc = f"- {c.name} ({c.data_type})"
            if c.comment:
                desc += f": {c.comment}"
            col_descriptions.append(desc)

        if len(self.columns) > 15:
            col_descriptions.append(f"... 以及另外 {len(self.columns) - 15} 個欄位")

        pk_text = f"主鍵：{', '.join(self.primary_key)}" if self.primary_key else ""

        return f"""資料表：{self.name}
描述：{self.comment or '無可用描述'}
{pk_text}
欄位：
{chr(10).join(col_descriptions)}
資料列數：{self.row_count or '未知'}""".strip()


class DDLExtractor:
    """從 Oracle 資料庫提取 DDL 中繼資料。"""

    # 使用 USER_* 視圖的 SQL 查詢（單一結構描述）
    TABLES_QUERY = """
        SELECT t.table_name, t.num_rows, tc.comments
        FROM user_tables t
        LEFT JOIN user_tab_comments tc ON t.table_name = tc.table_name
        ORDER BY t.table_name
    """

    COLUMNS_QUERY = """
        SELECT
            c.table_name,
            c.column_name,
            c.data_type ||
                CASE
                    WHEN c.data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR')
                        THEN '(' || c.data_length || ')'
                    WHEN c.data_type = 'NUMBER' AND c.data_precision IS NOT NULL
                        THEN '(' || c.data_precision ||
                             CASE WHEN c.data_scale > 0 THEN ',' || c.data_scale ELSE '' END || ')'
                    ELSE ''
                END as data_type,
            c.nullable,
            c.data_default,
            cc.comments
        FROM user_tab_columns c
        LEFT JOIN user_col_comments cc
            ON c.table_name = cc.table_name
            AND c.column_name = cc.column_name
        WHERE c.table_name = :table_name
        ORDER BY c.column_id
    """

    PRIMARY_KEY_QUERY = """
        SELECT cc.column_name
        FROM user_constraints c
        JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
        WHERE c.constraint_type = 'P'
        AND c.table_name = :table_name
        ORDER BY cc.position
    """

    INDEXES_QUERY = """
        SELECT
            i.index_name,
            i.uniqueness,
            LISTAGG(c.column_name, ',') WITHIN GROUP (ORDER BY c.column_position) as columns
        FROM user_indexes i
        JOIN user_ind_columns c ON i.index_name = c.index_name
        WHERE i.table_name = :table_name
        AND i.index_type = 'NORMAL'
        GROUP BY i.index_name, i.uniqueness
        ORDER BY i.index_name
    """

    def __init__(self, connection: oracledb.Connection):
        """以 Oracle 連線初始化。

        參數：
            connection: 活動的 Oracle 資料庫連線。
        """
        self._conn = connection

    def get_all_tables(self) -> list[TableInfo]:
        """提取所有資料表及其中繼資料。

        回傳：
            包含完整中繼資料的 TableInfo 物件列表。
        """
        cursor = self._conn.cursor()
        cursor.execute(self.TABLES_QUERY)

        tables = []
        for table_name, num_rows, comment in cursor:
            columns = self._get_columns(table_name)
            primary_key = self._get_primary_key(table_name)
            indexes = self._get_indexes(table_name)

            tables.append(TableInfo(
                name=table_name,
                comment=comment,
                row_count=num_rows,
                columns=columns,
                primary_key=primary_key,
                indexes=indexes,
            ))

        cursor.close()
        return tables

    def get_table_names(self) -> list[str]:
        """僅取得資料表名稱，不含完整中繼資料。

        回傳：
            資料表名稱列表。
        """
        cursor = self._conn.cursor()
        cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
        names = [row[0] for row in cursor]
        cursor.close()
        return names

    def get_table(self, table_name: str) -> Optional[TableInfo]:
        """取得特定資料表的中繼資料。

        參數：
            table_name: 資料表名稱（不分大小寫）。

        回傳：
            TableInfo 物件，若找不到則為 None。
        """
        table_name = table_name.upper()
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT t.table_name, t.num_rows, tc.comments
            FROM user_tables t
            LEFT JOIN user_tab_comments tc ON t.table_name = tc.table_name
            WHERE t.table_name = :table_name
        """, {"table_name": table_name})

        row = cursor.fetchone()
        cursor.close()

        if row is None:
            return None

        columns = self._get_columns(table_name)
        primary_key = self._get_primary_key(table_name)
        indexes = self._get_indexes(table_name)

        return TableInfo(
            name=row[0],
            comment=row[2],
            row_count=row[1],
            columns=columns,
            primary_key=primary_key,
            indexes=indexes,
        )

    def _get_columns(self, table_name: str) -> list[ColumnInfo]:
        """取得資料表的欄位中繼資料。"""
        cursor = self._conn.cursor()
        cursor.execute(self.COLUMNS_QUERY, {"table_name": table_name})

        columns = []
        for _, col_name, data_type, nullable, data_default, comment in cursor:
            # 清理 data_default（移除尾端空白）
            if data_default:
                data_default = data_default.strip()

            columns.append(ColumnInfo(
                name=col_name,
                data_type=data_type,
                nullable=(nullable == "Y"),
                data_default=data_default,
                comment=comment,
            ))

        cursor.close()
        return columns

    def _get_primary_key(self, table_name: str) -> list[str]:
        """取得資料表的主鍵欄位。"""
        cursor = self._conn.cursor()
        cursor.execute(self.PRIMARY_KEY_QUERY, {"table_name": table_name})
        pk_columns = [row[0] for row in cursor]
        cursor.close()
        return pk_columns

    def _get_indexes(self, table_name: str) -> list[IndexInfo]:
        """取得資料表的索引中繼資料。"""
        cursor = self._conn.cursor()
        cursor.execute(self.INDEXES_QUERY, {"table_name": table_name})

        indexes = []
        for idx_name, uniqueness, columns_str in cursor:
            indexes.append(IndexInfo(
                name=idx_name,
                columns=columns_str.split(","),
                is_unique=(uniqueness == "UNIQUE"),
            ))

        cursor.close()
        return indexes
