"""MCP Server for Oracle DDL RAG - exposes schema intelligence tools to AI assistants."""

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

# Initialize MCP Server
app = Server("oracle-ddl-rag")


# Tool definitions with schemas
TOOLS = [
    Tool(
        name="search_db_schema",
        description="""Search database schemas by business concept or table name.
USE THIS FIRST before writing any SQL to find relevant tables.
Examples: "customer orders", "payment transactions", "user authentication".""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language description or keywords to search for",
                },
                "limit": {
                    "type": "integer",
                    "description": f"Maximum results to return (default: {DEFAULT_SEARCH_LIMIT})",
                    "default": DEFAULT_SEARCH_LIMIT,
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="get_table_schema",
        description="""Get complete schema for a specific table.
Use this to get detailed column information BEFORE writing SQL that uses specific columns.""",
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Exact table name (case-insensitive)",
                },
                "include_indexes": {
                    "type": "boolean",
                    "description": "Whether to include index definitions",
                    "default": False,
                },
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="get_enum_values",
        description="""Get valid values for STATUS/TYPE columns.
ALWAYS USE THIS before filtering by status, type, or any enum-like column.
This prevents using invalid values that don't exist in the database.""",
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Table containing the column",
                },
                "column_name": {
                    "type": "string",
                    "description": "Column name (e.g., STATUS, TYPE, IS_ACTIVE)",
                },
            },
            "required": ["table_name", "column_name"],
        },
    ),
    Tool(
        name="get_join_pattern",
        description="""Get the correct JOIN condition between two tables.
ALWAYS USE THIS before writing JOINs to ensure correct column mappings.
This prevents hallucinated or incorrect join conditions.""",
        inputSchema={
            "type": "object",
            "properties": {
                "table_a": {
                    "type": "string",
                    "description": "First table name",
                },
                "table_b": {
                    "type": "string",
                    "description": "Second table name",
                },
            },
            "required": ["table_a", "table_b"],
        },
    ),
    Tool(
        name="find_join_path",
        description="""Find the shortest join path between two tables that may not be directly related.
Use this when tables don't have a direct foreign key but you need to JOIN them
through intermediate tables.""",
        inputSchema={
            "type": "object",
            "properties": {
                "source_table": {
                    "type": "string",
                    "description": "Starting table name",
                },
                "target_table": {
                    "type": "string",
                    "description": "Destination table name",
                },
                "max_hops": {
                    "type": "integer",
                    "description": f"Maximum intermediate tables (default: {DEFAULT_PATH_MAX_HOPS})",
                    "default": DEFAULT_PATH_MAX_HOPS,
                },
            },
            "required": ["source_table", "target_table"],
        },
    ),
    Tool(
        name="search_columns",
        description="""Search for columns across all tables by name or description.
Use this when looking for specific data fields without knowing which table contains them.""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Column name pattern or description (e.g., 'email', 'created date')",
                },
                "data_type": {
                    "type": "string",
                    "description": "Optional filter by Oracle data type (e.g., VARCHAR2, NUMBER, DATE)",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results (default: 20)",
                    "default": 20,
                },
            },
            "required": ["query"],
        },
    ),
]


@app.list_tools()
async def list_tools() -> list[Tool]:
    """Return list of available tools."""
    return TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls from MCP clients."""

    # Map tool names to handlers
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
            text=json.dumps({"error": f"Unknown tool: {name}"}, indent=2),
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
    """Entry point for the MCP server."""
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
