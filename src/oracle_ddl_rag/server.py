"""MCP 伺服器 - 為 AI 助手提供 Oracle DDL RAG 結構智慧工具。"""

import asyncio
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
import json

from .tools import (
    search_db_schema,
    get_table_schema,
    get_enum_values,
    get_join_pattern,
    find_join_path,
    search_columns,
)
from .config import DEFAULT_SEARCH_LIMIT, DEFAULT_PATH_MAX_HOPS

# 初始化 MCP 伺服器
app = Server("oracle-ddl-rag")


# 工具定義與結構描述
TOOLS = [
    Tool(
        name="search_db_schema",
        description="""依業務概念或資料表名稱搜尋資料庫結構。
在撰寫任何 SQL 之前請先使用此工具來尋找相關資料表。
範例：「客戶訂單」、「付款交易」、「使用者驗證」。""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "自然語言描述或關鍵字",
                },
                "limit": {
                    "type": "integer",
                    "description": f"最大回傳結果數（預設：{DEFAULT_SEARCH_LIMIT}）",
                    "default": DEFAULT_SEARCH_LIMIT,
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="get_table_schema",
        description="""取得特定資料表的完整結構。
在撰寫使用特定欄位的 SQL 之前，請使用此工具取得詳細欄位資訊。""",
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "精確的資料表名稱（不分大小寫）",
                },
                "include_indexes": {
                    "type": "boolean",
                    "description": "是否包含索引定義",
                    "default": False,
                },
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="get_enum_values",
        description="""取得 STATUS/TYPE 欄位的有效值。
在依狀態、類型或任何列舉型欄位篩選之前，請務必使用此工具。
這可以防止使用資料庫中不存在的無效值。""",
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "包含該欄位的資料表",
                },
                "column_name": {
                    "type": "string",
                    "description": "欄位名稱（例如：STATUS、TYPE、IS_ACTIVE）",
                },
            },
            "required": ["table_name", "column_name"],
        },
    ),
    Tool(
        name="get_join_pattern",
        description="""取得兩個資料表之間正確的 JOIN 條件。
在撰寫 JOIN 之前請務必使用此工具，以確保正確的欄位對應。
這可以防止幻覺或錯誤的 JOIN 條件。""",
        inputSchema={
            "type": "object",
            "properties": {
                "table_a": {
                    "type": "string",
                    "description": "第一個資料表名稱",
                },
                "table_b": {
                    "type": "string",
                    "description": "第二個資料表名稱",
                },
            },
            "required": ["table_a", "table_b"],
        },
    ),
    Tool(
        name="find_join_path",
        description="""尋找兩個可能沒有直接關聯的資料表之間的最短 JOIN 路徑。
當資料表沒有直接外鍵但需要透過中繼資料表進行 JOIN 時使用此工具。""",
        inputSchema={
            "type": "object",
            "properties": {
                "source_table": {
                    "type": "string",
                    "description": "起始資料表名稱",
                },
                "target_table": {
                    "type": "string",
                    "description": "目標資料表名稱",
                },
                "max_hops": {
                    "type": "integer",
                    "description": f"最大中繼資料表數（預設：{DEFAULT_PATH_MAX_HOPS}）",
                    "default": DEFAULT_PATH_MAX_HOPS,
                },
            },
            "required": ["source_table", "target_table"],
        },
    ),
    Tool(
        name="search_columns",
        description="""依名稱或描述在所有資料表中搜尋欄位。
當尋找特定資料欄位但不知道在哪個資料表時使用此工具。""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "欄位名稱模式或描述（例如：「email」、「建立日期」）",
                },
                "data_type": {
                    "type": "string",
                    "description": "依 Oracle 資料類型篩選（例如：VARCHAR2、NUMBER、DATE）",
                },
                "limit": {
                    "type": "integer",
                    "description": "最大結果數（預設：20）",
                    "default": 20,
                },
            },
            "required": ["query"],
        },
    ),
]


@app.list_tools()
async def list_tools() -> list[Tool]:
    """回傳可用工具列表。"""
    return TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """處理來自 MCP 客戶端的工具呼叫。"""

    # 將工具名稱對應到處理函數
    handlers = {
        "search_db_schema": lambda args: search_db_schema(
            query=args["query"],
            limit=args.get("limit", DEFAULT_SEARCH_LIMIT),
        ),
        "get_table_schema": lambda args: get_table_schema(
            table_name=args["table_name"],
            include_indexes=args.get("include_indexes", False),
        ),
        "get_enum_values": lambda args: get_enum_values(
            table_name=args["table_name"],
            column_name=args["column_name"],
        ),
        "get_join_pattern": lambda args: get_join_pattern(
            table_a=args["table_a"],
            table_b=args["table_b"],
        ),
        "find_join_path": lambda args: find_join_path(
            source_table=args["source_table"],
            target_table=args["target_table"],
            max_hops=args.get("max_hops", DEFAULT_PATH_MAX_HOPS),
        ),
        "search_columns": lambda args: search_columns(
            query=args["query"],
            data_type=args.get("data_type"),
            limit=args.get("limit", 20),
        ),
    }

    handler = handlers.get(name)
    if not handler:
        return [TextContent(
            type="text",
            text=json.dumps({"error": f"未知的工具：{name}"}, indent=2),
        )]

    try:
        result = await handler(arguments)
        return [TextContent(
            type="text",
            text=json.dumps(result, indent=2, ensure_ascii=False),
        )]
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({
                "error": str(e),
                "tool": name,
                "arguments": arguments,
            }, indent=2),
        )]


def main():
    """MCP 伺服器入口點。"""
    async def run():
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options(),
            )

    asyncio.run(run())


if __name__ == "__main__":
    main()
