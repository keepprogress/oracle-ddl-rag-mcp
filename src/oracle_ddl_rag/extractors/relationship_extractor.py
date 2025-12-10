"""Oracle foreign key relationship extractor."""

from dataclasses import dataclass
import oracledb


@dataclass
class ForeignKeyInfo:
    """Foreign key relationship metadata."""
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
        """Create natural language description for embedding."""
        child_cols = ", ".join(self.child_columns)
        parent_cols = ", ".join(self.parent_columns)

        join_condition = " AND ".join(
            f"{self.child_table}.{cc} = {self.parent_table}.{pc}"
            for cc, pc in zip(self.child_columns, self.parent_columns)
        )

        return f"""Foreign Key: {self.child_table} references {self.parent_table}
Constraint: {self.constraint_name}
Child columns: {self.child_table}.{child_cols}
Parent columns: {self.parent_table}.{parent_cols}
JOIN condition: {join_condition}"""


class RelationshipExtractor:
    """Extract foreign key relationships from Oracle database."""

    # Get all FK relationships with column mappings
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

    # Get columns for a specific FK constraint
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
        """Initialize with an Oracle connection.

        Args:
            connection: Active Oracle database connection.
        """
        self._conn = connection

    def get_all_relationships(self) -> list[ForeignKeyInfo]:
        """Extract all foreign key relationships.

        Returns:
            List of ForeignKeyInfo objects.
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
        """Get all FK relationships involving a specific table.

        Args:
            table_name: Table name (case-insensitive).

        Returns:
            List of ForeignKeyInfo where table is parent or child.
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
        """Get child and parent columns for a FK constraint.

        Args:
            constraint_name: FK constraint name.

        Returns:
            Tuple of (child_columns, parent_columns).
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
