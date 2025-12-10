# Oracle DDL RAG MCP 伺服器

一個 MCP（模型上下文協定）伺服器，為 AI 程式碼助手（Claude Code、Gemini CLI、GitHub Copilot）提供 Oracle 資料庫結構智慧。透過語意搜尋提供已驗證的資料表關聯、欄位定義和列舉值，防止 SQL 幻覺。

## 問題

當 AI 助手為大型資料庫（400+ 資料表）撰寫 SQL 時，經常會：
- 幻覺資料表關聯和 JOIN 條件
- 猜錯 STATUS/TYPE 欄位的值
- 引用不存在的欄位
- 在比較中使用錯誤的欄位類型

## 解決方案

此 MCP 伺服器提供 6 個工具，讓 AI 助手在撰寫 SQL 前驗證資料庫結構：

| 工具 | 用途 |
|------|------|
| `search_db_schema` | 用自然語言查詢尋找相關資料表 |
| `get_table_schema` | 取得資料表的完整欄位定義 |
| `get_enum_values` | 取得 STATUS/TYPE 欄位的有效值 |
| `get_join_pattern` | 取得兩個資料表之間正確的 JOIN 條件 |
| `find_join_path` | 尋找經過中繼資料表的多跳 JOIN 路徑 |
| `search_columns` | 依名稱/描述在所有資料表中搜尋欄位 |

## 安全性：憑證永不暴露

```
┌─────────────────┐     ┌─────────────────────┐     ┌──────────────┐
│   AI 助手       │ ←→  │    MCP 伺服器       │ ←→  │   本地資料   │
│  (Claude 等)    │     │   (無資料庫存取)    │     │   ChromaDB   │
└─────────────────┘     └─────────────────────┘     │   SQLite     │
                                                    └──────────────┘
                                                           ↑
                                                    離線資料注入
                                                    (使用憑證)
                                                           ↓
                                                    ┌──────────────┐
                                                    │    Oracle    │
                                                    │    資料庫    │
                                                    └──────────────┘
```

- 資料庫憑證只在**離線注入**時使用
- MCP 伺服器只讀取**預建的本地資料**
- AI 助手**永遠看不到**連線字串或密碼

## 快速開始

### 1. 安裝 UV（如果尚未安裝）

```bash
# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 複製並安裝相依套件

```bash
cd oracle-ddl-rag-mcp
uv sync
```

### 3. 注入您的資料庫結構（一次性）

```bash
uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user your_user
```

系統會提示您輸入密碼（不會儲存）。

### 4. 設定 Claude Code

加入到您的 Claude Code MCP 設定：

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

### 5. 重新啟動 Claude Code

重新啟動後，Claude Code 就能使用這 6 個資料庫結構工具。

## 工具使用範例

### search_db_schema
```
「尋找與客戶訂單相關的資料表」
→ 回傳：ORDERS、ORDER_ITEMS、CUSTOMERS 及相似度分數
```

### get_table_schema
```
「取得 ORDERS 資料表的結構」
→ 回傳：所有欄位的類型、註解、主鍵、關聯
```

### get_enum_values
```
「ORDERS.STATUS 有哪些有效值？」
→ 回傳：['DRAFT', 'PENDING', 'APPROVED', 'CANCELLED'] 及其含義
```

### get_join_pattern
```
「如何 JOIN ORDERS 和 CUSTOMERS？」
→ 回傳：「ORDERS.CUSTOMER_ID = CUSTOMERS.ID」及 SQL 範例
```

### find_join_path
```
「尋找從 ORDER_ITEMS 到 CUSTOMERS 的路徑」
→ 回傳：ORDER_ITEMS → ORDERS → CUSTOMERS 及所有 JOIN 條件
```

### search_columns
```
「尋找包含 email 的欄位」
→ 回傳：CUSTOMERS.EMAIL、USERS.EMAIL_ADDRESS 等
```

## 嵌入模型設定

伺服器會自動偵測要使用哪個嵌入模型：

| 條件 | 使用的模型 |
|------|-----------|
| 設定了 `OPENAI_API_KEY` 環境變數 | OpenAI `text-embedding-3-small` (512 維) |
| 沒有 API 金鑰 | 本地 `all-MiniLM-L6-v2` (384 維) |

使用 OpenAI 嵌入（品質較佳）：
```bash
export OPENAI_API_KEY=sk-...
uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user scott
```

## 手動列舉值覆寫

對於沒有 CHECK 約束的欄位，可在 `data/manual_overrides.yaml` 中新增值：

```yaml
ORDERS:
  STATUS:
    - code: "0"
      meaning: 草稿 - 訂單尚未提交
    - code: "1"
      meaning: 待處理 - 等待審核
    - code: "2"
      meaning: 已核准 - 準備處理
    - code: "9"
      meaning: 已取消

CUSTOMERS:
  IS_ACTIVE:
    - code: "Y"
      meaning: 活躍客戶
    - code: "N"
      meaning: 非活躍客戶
```

更新此檔案後需重新執行注入。

## 專案結構

```
oracle-ddl-rag-mcp/
├── pyproject.toml              # UV/Python 專案設定
├── src/
│   └── oracle_ddl_rag/
│       ├── server.py           # MCP 伺服器入口點
│       ├── config.py           # 設定（無憑證）
│       ├── tools/              # 6 個 MCP 工具實作
│       ├── extractors/         # Oracle DDL 提取
│       ├── storage/            # ChromaDB + SQLite
│       ├── graph/              # NetworkX 路徑尋找
│       └── embeddings/         # OpenAI/本地嵌入服務
├── scripts/
│   └── ingest_schema.py        # 離線資料注入
├── data/
│   ├── chroma_db/              # 向量嵌入（已加入 gitignore）
│   ├── metadata.db             # SQLite 快取（已加入 gitignore）
│   └── manual_overrides.yaml   # 手動列舉定義
└── tests/
```

## 更新資料庫結構

當資料庫結構變更時，重新執行注入：

```bash
# 增量更新（保留現有資料）
uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user scott

# 完整重建（清除並重建）
uv run scripts/ingest_schema.py --dsn localhost:1521/ORCL --user scott --clear
```

## 疑難排解

### 搜尋結果顯示「找不到資料表」
- 確認注入已成功完成
- 檢查 `data/chroma_db/` 和 `data/metadata.db` 是否存在
- 嘗試使用 `--clear` 旗標重新執行注入

### Oracle 連線錯誤
- 確認已安裝 Oracle Instant Client
- 檢查 DSN 格式：`host:port/service_name`
- 確認與資料庫的網路連線

### OpenAI 嵌入錯誤
- 確認 `OPENAI_API_KEY` 設定正確
- 檢查 API 金鑰有嵌入權限
- 取消設定環境變數以改用本地模型

## 系統需求

- Python 3.11+
- Oracle 資料庫（僅供注入使用）
- UV 套件管理器
- 選用：OpenAI API 金鑰（可獲得更好的嵌入品質）

## 授權

MIT
