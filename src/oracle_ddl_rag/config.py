"""Oracle DDL RAG MCP 伺服器的非敏感設定。"""

from pathlib import Path

# 專案路徑
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CHROMA_PATH = DATA_DIR / "chroma_db"
SQLITE_PATH = DATA_DIR / "metadata.db"
MANUAL_OVERRIDES_PATH = DATA_DIR / "manual_overrides.yaml"

# ChromaDB 集合名稱
COLLECTION_TABLES = "tables"
COLLECTION_COLUMNS = "columns"
COLLECTION_RELATIONSHIPS = "relationships"

# 嵌入模型設定
OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"
OPENAI_EMBEDDING_DIMS = 512
LOCAL_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# 搜尋預設值
DEFAULT_SEARCH_LIMIT = 10
MAX_SEARCH_LIMIT = 50
DEFAULT_PATH_MAX_HOPS = 4
