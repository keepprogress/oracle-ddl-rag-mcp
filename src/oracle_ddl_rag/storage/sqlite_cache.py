"""用於快速結構化中繼資料查詢的 SQLite 快取。"""

import json
from datetime import datetime
from typing import Optional
from pathlib import Path

from sqlalchemy import create_engine, Column, String, Text, Integer, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from ..config import SQLITE_PATH, DATA_DIR

Base = declarative_base()


class TableModel(Base):
    """資料表中繼資料模型。"""
    __tablename__ = "tables"

    table_name = Column(String(128), primary_key=True)
    columns_json = Column(Text)  # JSON 欄位定義列表
    primary_key_json = Column(Text)  # JSON 主鍵欄位列表
    comment = Column(Text)
    row_count = Column(Integer)
    indexes_json = Column(Text)  # JSON 索引列表
    last_synced = Column(DateTime, default=datetime.utcnow)

    @property
    def columns(self) -> list[dict]:
        return json.loads(self.columns_json) if self.columns_json else []

    @columns.setter
    def columns(self, value: list[dict]):
        self.columns_json = json.dumps(value)

    @property
    def primary_key(self) -> list[str]:
        return json.loads(self.primary_key_json) if self.primary_key_json else []

    @primary_key.setter
    def primary_key(self, value: list[str]):
        self.primary_key_json = json.dumps(value)

    @property
    def indexes(self) -> list[dict]:
        return json.loads(self.indexes_json) if self.indexes_json else []

    @indexes.setter
    def indexes(self, value: list[dict]):
        self.indexes_json = json.dumps(value)


class EnumModel(Base):
    """STATUS/TYPE 欄位的列舉值。"""
    __tablename__ = "enums"

    id = Column(String(256), primary_key=True)  # TABLE_NAME.COLUMN_NAME
    table_name = Column(String(128), index=True)
    column_name = Column(String(128))
    values_json = Column(Text)  # JSON {code, meaning} 列表
    source = Column(String(50))  # 'check_constraint'、'manual'
    last_synced = Column(DateTime, default=datetime.utcnow)

    @property
    def values(self) -> list[dict]:
        return json.loads(self.values_json) if self.values_json else []

    @values.setter
    def values(self, value: list[dict]):
        self.values_json = json.dumps(value)


class RelationshipModel(Base):
    """資料表之間的外鍵關聯。"""
    __tablename__ = "relationships"

    id = Column(String(256), primary_key=True)  # CHILD_TABLE->PARENT_TABLE
    parent_table = Column(String(128), index=True)
    child_table = Column(String(128), index=True)
    parent_columns_json = Column(Text)  # JSON 列表
    child_columns_json = Column(Text)  # JSON 列表
    constraint_name = Column(String(128))
    last_synced = Column(DateTime, default=datetime.utcnow)

    @property
    def parent_columns(self) -> list[str]:
        return json.loads(self.parent_columns_json) if self.parent_columns_json else []

    @parent_columns.setter
    def parent_columns(self, value: list[str]):
        self.parent_columns_json = json.dumps(value)

    @property
    def child_columns(self) -> list[str]:
        return json.loads(self.child_columns_json) if self.child_columns_json else []

    @child_columns.setter
    def child_columns(self, value: list[str]):
        self.child_columns_json = json.dumps(value)


class SyncMetadataModel(Base):
    """追蹤同步中繼資料。"""
    __tablename__ = "sync_metadata"

    key = Column(String(64), primary_key=True)
    value = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow)


class SQLiteCache:
    """使用 SQLite 的快速結構化中繼資料快取。"""

    def __init__(self, path: Optional[str] = None):
        """初始化 SQLite 資料庫。

        參數：
            path: SQLite 檔案的覆寫路徑。若為 None 則使用預設值。
        """
        db_path = path or str(SQLITE_PATH)
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def _get_session(self) -> Session:
        return self.SessionLocal()

    # ========== 資料表操作 ==========

    def upsert_table(self, data: dict) -> None:
        """插入或更新資料表記錄。

        參數：
            data: 包含 table_name、columns、primary_key、comment 等的字典。
        """
        with self._get_session() as session:
            table = session.get(TableModel, data["table_name"].upper())
            if table is None:
                table = TableModel(table_name=data["table_name"].upper())
                session.add(table)

            table.columns = data.get("columns", [])
            table.primary_key = data.get("primary_key", [])
            table.comment = data.get("comment")
            table.row_count = data.get("row_count")
            table.indexes = data.get("indexes", [])
            table.last_synced = datetime.utcnow()
            session.commit()

    def get_table(self, table_name: str) -> Optional[dict]:
        """依名稱取得資料表中繼資料。

        參數：
            table_name: 資料表名稱（不分大小寫）。

        回傳：
            包含資料表中繼資料的字典，若找不到則為 None。
        """
        with self._get_session() as session:
            table = session.get(TableModel, table_name.upper())
            if table:
                return {
                    "table_name": table.table_name,
                    "columns": table.columns,
                    "primary_key": table.primary_key,
                    "comment": table.comment,
                    "row_count": table.row_count,
                    "indexes": table.indexes,
                }
            return None

    def get_all_tables(self) -> list[dict]:
        """取得所有資料表名稱。"""
        with self._get_session() as session:
            tables = session.query(TableModel).all()
            return [
                {
                    "table_name": t.table_name,
                    "comment": t.comment,
                    "row_count": t.row_count,
                }
                for t in tables
            ]

    # ========== 列舉操作 ==========

    def upsert_enum(self, data: dict) -> None:
        """插入或更新欄位的列舉值。

        參數：
            data: 包含 table_name、column_name、values、source 的字典。
        """
        with self._get_session() as session:
            enum_id = f"{data['table_name'].upper()}.{data['column_name'].upper()}"
            enum = session.get(EnumModel, enum_id)
            if enum is None:
                enum = EnumModel(id=enum_id)
                session.add(enum)

            enum.table_name = data["table_name"].upper()
            enum.column_name = data["column_name"].upper()
            enum.values = data.get("values", [])
            enum.source = data.get("source", "unknown")
            enum.last_synced = datetime.utcnow()
            session.commit()

    def get_enum(self, table_name: str, column_name: str) -> Optional[dict]:
        """取得特定欄位的列舉值。

        參數：
            table_name: 資料表名稱（不分大小寫）。
            column_name: 欄位名稱（不分大小寫）。

        回傳：
            包含列舉值的字典，若找不到則為 None。
        """
        with self._get_session() as session:
            enum_id = f"{table_name.upper()}.{column_name.upper()}"
            enum = session.get(EnumModel, enum_id)
            if enum:
                return {
                    "table_name": enum.table_name,
                    "column_name": enum.column_name,
                    "values": enum.values,
                    "source": enum.source,
                }
            return None

    # ========== 關聯操作 ==========

    def upsert_relationship(self, data: dict) -> None:
        """插入或更新外鍵關聯。

        參數：
            data: 包含 parent_table、child_table、columns 等的字典。
        """
        with self._get_session() as session:
            parent = data["parent_table"].upper()
            child = data["child_table"].upper()
            rel_id = f"{child}->{parent}"

            rel = session.get(RelationshipModel, rel_id)
            if rel is None:
                rel = RelationshipModel(id=rel_id)
                session.add(rel)

            rel.parent_table = parent
            rel.child_table = child
            rel.parent_columns = data.get("parent_columns", [])
            rel.child_columns = data.get("child_columns", [])
            rel.constraint_name = data.get("constraint_name")
            rel.last_synced = datetime.utcnow()
            session.commit()

    def get_relationship(self, table_a: str, table_b: str) -> Optional[dict]:
        """取得兩個資料表之間的直接外鍵關聯。

        參數：
            table_a: 第一個資料表名稱（不分大小寫）。
            table_b: 第二個資料表名稱（不分大小寫）。

        回傳：
            包含關聯詳情的字典，若找不到則為 None。
        """
        a, b = table_a.upper(), table_b.upper()
        with self._get_session() as session:
            # 嘗試兩個方向
            for parent, child in [(a, b), (b, a)]:
                rel_id = f"{child}->{parent}"
                rel = session.get(RelationshipModel, rel_id)
                if rel:
                    return {
                        "parent_table": rel.parent_table,
                        "child_table": rel.child_table,
                        "parent_columns": rel.parent_columns,
                        "child_columns": rel.child_columns,
                        "constraint_name": rel.constraint_name,
                    }
            return None

    def get_all_relationships(self) -> list[dict]:
        """取得所有外鍵關聯。"""
        with self._get_session() as session:
            rels = session.query(RelationshipModel).all()
            return [
                {
                    "parent_table": r.parent_table,
                    "child_table": r.child_table,
                    "parent_columns": r.parent_columns,
                    "child_columns": r.child_columns,
                    "constraint_name": r.constraint_name,
                }
                for r in rels
            ]

    def get_table_relationships(self, table_name: str) -> list[dict]:
        """取得涉及特定資料表的所有關聯。

        參數：
            table_name: 資料表名稱（不分大小寫）。

        回傳：
            該資料表作為父表或子表的關聯列表。
        """
        table_name = table_name.upper()
        with self._get_session() as session:
            rels = session.query(RelationshipModel).filter(
                (RelationshipModel.parent_table == table_name) |
                (RelationshipModel.child_table == table_name)
            ).all()
            return [
                {
                    "parent_table": r.parent_table,
                    "child_table": r.child_table,
                    "parent_columns": r.parent_columns,
                    "child_columns": r.child_columns,
                    "constraint_name": r.constraint_name,
                }
                for r in rels
            ]

    # ========== 同步中繼資料操作 ==========

    def get_last_sync_time(self) -> Optional[datetime]:
        """取得上次成功同步的時間戳記。"""
        with self._get_session() as session:
            meta = session.get(SyncMetadataModel, "last_sync_time")
            if meta:
                return datetime.fromisoformat(meta.value)
            return None

    def update_last_sync_time(self) -> None:
        """將上次同步時間戳記更新為現在。"""
        with self._get_session() as session:
            meta = session.get(SyncMetadataModel, "last_sync_time")
            if meta is None:
                meta = SyncMetadataModel(key="last_sync_time")
                session.add(meta)
            meta.value = datetime.utcnow().isoformat()
            meta.updated_at = datetime.utcnow()
            session.commit()

    def clear_all(self) -> None:
        """刪除所有快取資料。"""
        with self._get_session() as session:
            session.query(TableModel).delete()
            session.query(EnumModel).delete()
            session.query(RelationshipModel).delete()
            session.query(SyncMetadataModel).delete()
            session.commit()

    def get_stats(self) -> dict:
        """取得快取資料的統計資訊。"""
        with self._get_session() as session:
            return {
                "tables": session.query(TableModel).count(),
                "enums": session.query(EnumModel).count(),
                "relationships": session.query(RelationshipModel).count(),
                "last_sync": self.get_last_sync_time(),
            }
