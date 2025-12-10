"""Oracle DDL extractor for tables, columns, and indexes."""

from typing import Optional
from dataclasses import dataclass
import oracledb


@dataclass
class ColumnInfo:
    """Column metadata."""
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
    """Index metadata."""
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
    """Table metadata with columns."""
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
        """Create natural language description for embedding."""
        col_descriptions = []
        for c in self.columns[:15]:  # Limit for embedding size
            desc = f"- {c.name} ({c.data_type})"
            if c.comment:
                desc += f": {c.comment}"
            col_descriptions.append(desc)

        if len(self.columns) > 15:
            col_descriptions.append(f"... and {len(self.columns) - 15} more columns")

        pk_text = f"Primary Key: {', '.join(self.primary_key)}" if self.primary_key else ""

        return f"""Table: {self.name}
Description: {self.comment or 'No description available'}
{pk_text}
Columns:
{chr(10).join(col_descriptions)}
Row Count: {self.row_count or 'Unknown'}""".strip()


class DDLExtractor:
    """Extract DDL metadata from Oracle database."""

    # SQL Queries using USER_* views (single schema)
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
        """Initialize with an Oracle connection.

        Args:
            connection: Active Oracle database connection.
        """
        self._conn = connection

    def get_all_tables(self) -> list[TableInfo]:
        """Extract all tables with their metadata.

        Returns:
            List of TableInfo objects with full metadata.
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
        """Get just table names without full metadata.

        Returns:
            List of table names.
        """
        cursor = self._conn.cursor()
        cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
        names = [row[0] for row in cursor]
        cursor.close()
        return names

    def get_table(self, table_name: str) -> Optional[TableInfo]:
        """Get metadata for a specific table.

        Args:
            table_name: Name of the table (case-insensitive).

        Returns:
            TableInfo object or None if not found.
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
        """Get column metadata for a table."""
        cursor = self._conn.cursor()
        cursor.execute(self.COLUMNS_QUERY, {"table_name": table_name})

        columns = []
        for _, col_name, data_type, nullable, data_default, comment in cursor:
            # Clean up data_default (remove trailing spaces)
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
        """Get primary key columns for a table."""
        cursor = self._conn.cursor()
        cursor.execute(self.PRIMARY_KEY_QUERY, {"table_name": table_name})
        pk_columns = [row[0] for row in cursor]
        cursor.close()
        return pk_columns

    def _get_indexes(self, table_name: str) -> list[IndexInfo]:
        """Get index metadata for a table."""
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
