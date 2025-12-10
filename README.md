# Oracle DDL RAG MCP Server

An MCP (Model Context Protocol) server that provides Oracle database schema intelligence to AI coding assistants (Claude Code, Gemini CLI, GitHub Copilot). It prevents SQL hallucinations by providing verified table relationships, column definitions, and enum values through semantic search.

## Problem

When AI assistants write SQL for large databases (400+ tables), they often:
- Hallucinate table relationships and JOIN conditions
- Guess wrong values for STATUS/TYPE columns
- Reference columns that don't exist
- Use incorrect column types in comparisons

## Solution

This MCP server exposes 6 tools that AI assistants can use to verify database schema before writing SQL:

| Tool | Purpose |
|------|---------|
| `search_db_schema` | Find relevant tables by natural language query |
| `get_table_schema` | Get complete column definitions for a table |
| `get_enum_values` | Get valid values for STATUS/TYPE columns |
| `get_join_pattern` | Get correct JOIN condition between two tables |
| `find_join_path` | Find multi-hop join path through intermediate tables |
| `search_columns` | Find columns across all tables by name/description |

## Security: Credentials Never Exposed

```
┌─────────────────┐     ┌─────────────────────┐     ┌──────────────┐
│  AI Assistants  │ ←→  │   MCP Server        │ ←→  │  Local Data  │
│  (Claude, etc.) │     │   (No DB Access)    │     │  ChromaDB    │
└─────────────────┘     └─────────────────────┘     │  SQLite      │
                                                    └──────────────┘
                                                           ↑
                                                    Offline Ingestion
                                                    (with credentials)
                                                           ↓
                                                    ┌──────────────┐
                                                    │    Oracle    │
                                                    │   Database   │
                                                    └──────────────┘
```

- Database credentials are only used during **offline ingestion**
- The MCP server reads **pre-built local data** only
- AI assistants **never see** connection strings or passwords

## Quick Start

### 1. Install UV (if not already installed)

```bash
# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Clone and Install Dependencies

```bash
cd oracle-ddl-rag-mcp
uv sync
```

### 3. Ingest Your Schema (One-Time)

```bash
uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user your_user
```

You'll be prompted for your password (not stored).

### 4. Configure Claude Code

Add to your Claude Code MCP configuration:

**Windows** (`%APPDATA%\Claude\claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "oracle-ddl": {
      "command": "uv",
      "args": ["run", "oracle-ddl-mcp"],
      "cwd": "C:/Developer/oracle-ddl-rag-mcp"
    }
  }
}
```

**macOS/Linux** (`~/.config/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "oracle-ddl": {
      "command": "uv",
      "args": ["run", "oracle-ddl-mcp"],
      "cwd": "/path/to/oracle-ddl-rag-mcp"
    }
  }
}
```

### 5. Restart Claude Code

After restarting, Claude Code will have access to the 6 database schema tools.

## Tool Usage Examples

### search_db_schema
```
"Find tables related to customer orders"
→ Returns: ORDERS, ORDER_ITEMS, CUSTOMERS with similarity scores
```

### get_table_schema
```
"Get schema for ORDERS table"
→ Returns: All columns with types, comments, primary key, relationships
```

### get_enum_values
```
"What are the valid values for ORDERS.STATUS?"
→ Returns: ['DRAFT', 'PENDING', 'APPROVED', 'CANCELLED'] with meanings
```

### get_join_pattern
```
"How do I join ORDERS and CUSTOMERS?"
→ Returns: "ORDERS.CUSTOMER_ID = CUSTOMERS.ID" with SQL example
```

### find_join_path
```
"Find path from ORDER_ITEMS to CUSTOMERS"
→ Returns: ORDER_ITEMS → ORDERS → CUSTOMERS with all join conditions
```

### search_columns
```
"Find columns containing email addresses"
→ Returns: CUSTOMERS.EMAIL, USERS.EMAIL_ADDRESS, etc.
```

## Embedding Model Configuration

The server auto-detects which embedding model to use:

| Condition | Model Used |
|-----------|------------|
| `OPENAI_API_KEY` env var set | OpenAI `text-embedding-3-small` (512 dims) |
| No API key | Local `all-MiniLM-L6-v2` (384 dims) |

For OpenAI embeddings (better quality):
```bash
export OPENAI_API_KEY=sk-...
uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user scott
```

## Manual Enum Overrides

For columns without CHECK constraints, add values to `data/manual_overrides.yaml`:

```yaml
ORDERS:
  STATUS:
    - code: "0"
      meaning: Draft - order not yet submitted
    - code: "1"
      meaning: Pending - awaiting approval
    - code: "2"
      meaning: Approved - ready for processing
    - code: "9"
      meaning: Cancelled

CUSTOMERS:
  IS_ACTIVE:
    - code: "Y"
      meaning: Active customer
    - code: "N"
      meaning: Inactive customer
```

Re-run ingestion after updating this file.

## Project Structure

```
oracle-ddl-rag-mcp/
├── pyproject.toml              # UV/Python project config
├── src/
│   └── oracle_ddl_rag/
│       ├── server.py           # MCP server entry point
│       ├── config.py           # Configuration (no credentials)
│       ├── tools/              # 6 MCP tool implementations
│       ├── extractors/         # Oracle DDL extraction
│       ├── storage/            # ChromaDB + SQLite
│       ├── graph/              # NetworkX for path finding
│       └── embeddings/         # OpenAI/local embedding service
├── scripts/
│   └── ingest_schema.py        # Offline data ingestion
├── data/
│   ├── chroma_db/              # Vector embeddings (gitignored)
│   ├── metadata.db             # SQLite cache (gitignored)
│   └── manual_overrides.yaml   # Manual enum definitions
└── tests/
```

## Updating Schema

When your database schema changes, re-run the ingestion:

```bash
# Incremental update (keeps existing data)
uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user scott

# Full refresh (clears and rebuilds)
uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user scott --clear
```

## Troubleshooting

### "No tables found" in search results
- Verify ingestion completed successfully
- Check that `data/chroma_db/` and `data/metadata.db` exist
- Try re-running ingestion with `--clear` flag

### Oracle connection errors
- Ensure Oracle Instant Client is installed
- Check DSN format: `host:port/service_name`
- Verify network connectivity to the database

### Embedding errors with OpenAI
- Verify `OPENAI_API_KEY` is set correctly
- Check API key has embeddings permissions
- Fall back to local model by unsetting the env var

## Requirements

- Python 3.11+
- Oracle database (for ingestion only)
- UV package manager
- Optional: OpenAI API key for better embeddings

## License

MIT
