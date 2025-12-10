"""從 CHECK 約束提取 Oracle 列舉值。"""

import re
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

import yaml
import oracledb

from ..config import MANUAL_OVERRIDES_PATH


@dataclass
class EnumValue:
    """單一列舉值及選用的含義。"""
    code: str
    meaning: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "meaning": self.meaning,
        }


@dataclass
class EnumInfo:
    """欄位的列舉值。"""
    table_name: str
    column_name: str
    values: list[EnumValue]
    source: str  # 'check_constraint' 或 'manual'

    def to_dict(self) -> dict:
        return {
            "table_name": self.table_name,
            "column_name": self.column_name,
            "values": [v.to_dict() for v in self.values],
            "source": self.source,
        }

    def to_document(self) -> str:
        """建立用於嵌入的自然語言描述。"""
        values_text = "\n".join(
            f"- {v.code}: {v.meaning or '無描述'}"
            for v in self.values
        )
        return f"""{self.table_name}.{self.column_name} 的列舉值：
來源：{self.source}
有效值：
{values_text}"""


class EnumExtractor:
    """從 Oracle CHECK 約束和手動定義提取列舉值。"""

    # 查詢含有 IN 子句的 CHECK 約束
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
        """初始化列舉提取器。

        參數：
            connection: 選用的 Oracle 連線，用於 CHECK 約束提取。
                       若為 None，則只載入手動覆寫。
        """
        self._conn = connection

    def extract_all(self) -> list[EnumInfo]:
        """從 CHECK 約束和手動覆寫提取所有列舉值。

        回傳：
            EnumInfo 物件列表。
        """
        enums: dict[str, EnumInfo] = {}  # 以 TABLE.COLUMN 為鍵

        # 首先從 CHECK 約束提取（如果有連線的話）
        if self._conn:
            for enum in self._extract_from_check_constraints():
                key = f"{enum.table_name}.{enum.column_name}"
                enums[key] = enum

        # 載入手動覆寫（可覆寫或補充 CHECK 約束）
        for enum in self._load_manual_overrides():
            key = f"{enum.table_name}.{enum.column_name}"
            if key in enums:
                # 合併：保留 CHECK 的代碼，從手動添加含義
                existing = enums[key]
                manual_meanings = {v.code: v.meaning for v in enum.values}
                for value in existing.values:
                    if value.code in manual_meanings:
                        value.meaning = manual_meanings[value.code]
            else:
                enums[key] = enum

        return list(enums.values())

    def _extract_from_check_constraints(self) -> list[EnumInfo]:
        """從 CHECK 約束提取列舉值。"""
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
        """解析 CHECK 約束以提取欄位名稱和有效值。

        處理以下模式：
        - STATUS IN ('ACTIVE', 'INACTIVE')
        - "STATUS" IN ('A', 'B', 'C')
        - TYPE IN (1, 2, 3)

        參數：
            search_condition: Oracle CHECK 約束條件。

        回傳：
            (欄位名稱, 值列表) 的元組，若非 IN 約束則為 None。
        """
        # 模式：column_name IN (values)
        # 欄位名稱可能有引號或無引號
        pattern = r'["\']?(\w+)["\']?\s+IN\s*\(\s*([^)]+)\s*\)'
        match = re.search(pattern, search_condition, re.IGNORECASE)

        if not match:
            return None

        column_name = match.group(1)
        values_str = match.group(2)

        # 提取值（引號字串或數字）
        values = []

        # 先嘗試引號字串：'value1', 'value2'
        quoted_values = re.findall(r"'([^']*)'", values_str)
        if quoted_values:
            values = quoted_values
        else:
            # 嘗試無引號數字：1, 2, 3
            number_values = re.findall(r'\b(\d+)\b', values_str)
            if number_values:
                values = number_values

        if values:
            return column_name, values
        return None

    def _load_manual_overrides(self) -> list[EnumInfo]:
        """從 YAML 檔案載入手動列舉定義。"""
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
        """取得特定欄位的列舉值。

        參數：
            table_name: 資料表名稱（不分大小寫）。
            column_name: 欄位名稱（不分大小寫）。

        回傳：
            EnumInfo，若無定義列舉則為 None。
        """
        all_enums = self.extract_all()
        key = f"{table_name.upper()}.{column_name.upper()}"

        for enum in all_enums:
            if f"{enum.table_name}.{enum.column_name}" == key:
                return enum
        return None
