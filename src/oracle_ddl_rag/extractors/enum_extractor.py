"""Oracle enum value extractor from CHECK constraints."""

import re
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

import yaml
import oracledb

from ..config import MANUAL_OVERRIDES_PATH


@dataclass
class EnumValue:
    """Single enum value with optional meaning."""
    code: str
    meaning: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "meaning": self.meaning,
        }


@dataclass
class EnumInfo:
    """Enum values for a column."""
    table_name: str
    column_name: str
    values: list[EnumValue]
    source: str  # 'check_constraint' or 'manual'

    def to_dict(self) -> dict:
        return {
            "table_name": self.table_name,
            "column_name": self.column_name,
            "values": [v.to_dict() for v in self.values],
            "source": self.source,
        }

    def to_document(self) -> str:
        """Create natural language description for embedding."""
        values_text = "\n".join(
            f"- {v.code}: {v.meaning or 'No description'}"
            for v in self.values
        )
        return f"""Enum values for {self.table_name}.{self.column_name}:
Source: {self.source}
Valid values:
{values_text}"""


class EnumExtractor:
    """Extract enum values from Oracle CHECK constraints and manual definitions."""

    # Query CHECK constraints with IN clauses
    CHECK_CONSTRAINT_QUERY = """
        SELECT
            c.table_name,
            c.constraint_name,
            c.search_condition
        FROM user_constraints c
        WHERE c.constraint_type = 'C'
        AND c.search_condition IS NOT NULL
        ORDER BY c.table_name
    """

    def __init__(self, connection: Optional[oracledb.Connection] = None):
        """Initialize enum extractor.

        Args:
            connection: Optional Oracle connection for CHECK constraint extraction.
                       If None, only manual overrides will be loaded.
        """
        self._conn = connection

    def extract_all(self) -> list[EnumInfo]:
        """Extract all enum values from CHECK constraints and manual overrides.

        Returns:
            List of EnumInfo objects.
        """
        enums: dict[str, EnumInfo] = {}  # keyed by TABLE.COLUMN

        # Extract from CHECK constraints first (if connection available)
        if self._conn:
            for enum in self._extract_from_check_constraints():
                key = f"{enum.table_name}.{enum.column_name}"
                enums[key] = enum

        # Load manual overrides (can override or supplement CHECK constraints)
        for enum in self._load_manual_overrides():
            key = f"{enum.table_name}.{enum.column_name}"
            if key in enums:
                # Merge: keep codes from CHECK, add meanings from manual
                existing = enums[key]
                manual_meanings = {v.code: v.meaning for v in enum.values}
                for value in existing.values:
                    if value.code in manual_meanings:
                        value.meaning = manual_meanings[value.code]
            else:
                enums[key] = enum

        return list(enums.values())

    def _extract_from_check_constraints(self) -> list[EnumInfo]:
        """Extract enum values from CHECK constraints."""
        cursor = self._conn.cursor()
        cursor.execute(self.CHECK_CONSTRAINT_QUERY)

        enums = []
        for table_name, constraint_name, search_condition in cursor:
            if search_condition is None:
                continue

            parsed = self._parse_check_constraint(search_condition)
            if parsed:
                column_name, values = parsed
                enums.append(EnumInfo(
                    table_name=table_name,
                    column_name=column_name,
                    values=[EnumValue(code=v) for v in values],
                    source="check_constraint",
                ))

        cursor.close()
        return enums

    def _parse_check_constraint(
        self, search_condition: str
    ) -> Optional[tuple[str, list[str]]]:
        """Parse CHECK constraint to extract column name and valid values.

        Handles patterns like:
        - STATUS IN ('ACTIVE', 'INACTIVE')
        - "STATUS" IN ('A', 'B', 'C')
        - TYPE IN (1, 2, 3)

        Args:
            search_condition: Oracle CHECK constraint condition.

        Returns:
            Tuple of (column_name, values) or None if not an IN constraint.
        """
        # Pattern: column_name IN (values)
        # Column name can be quoted or unquoted
        pattern = r'["\']?(\w+)["\']?\s+IN\s*\(\s*([^)]+)\s*\)'
        match = re.search(pattern, search_condition, re.IGNORECASE)

        if not match:
            return None

        column_name = match.group(1)
        values_str = match.group(2)

        # Extract values (quoted strings or numbers)
        values = []

        # Try quoted strings first: 'value1', 'value2'
        quoted_values = re.findall(r"'([^']*)'", values_str)
        if quoted_values:
            values = quoted_values
        else:
            # Try unquoted numbers: 1, 2, 3
            number_values = re.findall(r'\b(\d+)\b', values_str)
            if number_values:
                values = number_values

        if values:
            return column_name, values
        return None

    def _load_manual_overrides(self) -> list[EnumInfo]:
        """Load manual enum definitions from YAML file."""
        if not MANUAL_OVERRIDES_PATH.exists():
            return []

        try:
            content = yaml.safe_load(MANUAL_OVERRIDES_PATH.read_text(encoding="utf-8"))
            if not content:
                return []
        except Exception:
            return []

        enums = []
        for table_name, columns in content.items():
            if not isinstance(columns, dict):
                continue

            for column_name, values_list in columns.items():
                if not isinstance(values_list, list):
                    continue

                values = []
                for v in values_list:
                    if isinstance(v, dict):
                        values.append(EnumValue(
                            code=str(v.get("code", "")),
                            meaning=v.get("meaning"),
                        ))

                if values:
                    enums.append(EnumInfo(
                        table_name=table_name.upper(),
                        column_name=column_name.upper(),
                        values=values,
                        source="manual",
                    ))

        return enums

    def get_enum_for_column(
        self, table_name: str, column_name: str
    ) -> Optional[EnumInfo]:
        """Get enum values for a specific column.

        Args:
            table_name: Table name (case-insensitive).
            column_name: Column name (case-insensitive).

        Returns:
            EnumInfo or None if no enum defined.
        """
        all_enums = self.extract_all()
        key = f"{table_name.upper()}.{column_name.upper()}"

        for enum in all_enums:
            if f"{enum.table_name}.{enum.column_name}" == key:
                return enum
        return None
