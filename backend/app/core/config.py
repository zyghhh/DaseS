"""
应用核心配置
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # --- 基本信息 ---
    PROJECT_NAME: str = "DaseS"
    VERSION: str = "0.1.0"
    DEBUG: bool = True

    # --- CORS ---
    CORS_ORIGINS: list[str] = ["http://localhost:3000"]

    # --- 数据库 ---
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/dases"

    # --- Redis ---
    REDIS_URL: str = "redis://localhost:6379/0"

    # --- 向量检索（通过 Elasticsearch 8.x kNN 实现，暂无独立向量库）---
    # 设置 ES_KNN_DIMS 与向量字段名称即可开启 kNN，无需额外服务
    ES_KNN_DIMS: int = 1024            # BAAI/bge-large-en-v1.5 输出维度
    ES_VECTOR_FIELD: str = "embedding" # 向量字段名（待添加到索引 mapping）

    # --- LLM ---
    LLM_API_KEY: str = ""
    LLM_MODEL: str = "gpt-4o"
    LLM_BASE_URL: str = ""

    # --- Embedding ---
    EMBEDDING_MODEL: str = "BAAI/bge-large-en-v1.5"

    # --- LangFuse (可观测性) ---
    LANGFUSE_SECRET_KEY: str = ""
    LANGFUSE_PUBLIC_KEY: str = ""
    LANGFUSE_HOST: str = "http://localhost:3001"

    # --- Elasticsearch ---
    ES_HOST: str = "http://49.52.27.139:9200"  # 生产地址；本地开发可用 SSH 隧道: http://localhost:19200
    ES_INDEX_PAPERS: str = "dblp_papers"        # 遗留配置，兼容旧代码
    ES_ALIAS_PAPERS: str = "dblp_search"         # 蓝绿发布别名，所有查询走此别名
    ES_INDEX_PREFIX: str = "dblp_index"           # 蓝绿索引前缀，实际索引名: dblp_index_YYYYMMDD

    # --- MCP Server ---
    MCP_AUTH_ENABLED: bool = False
    MCP_AUTH_PATH_PREFIX: str = "/mcp"
    MCP_API_KEYS: str = ""  # JSON: {"sk-xxx": {"user": "alice", "quota": 100}} 或逗号分隔
    MCP_DEFAULT_QUOTA: int = 1000
    MCP_QUOTA_BACKEND: str = "mock"  # mock | redis
    MCP_QUOTA_REDIS_PREFIX: str = "dases:mcp:quota"
    MCP_REDIS_FALLBACK_TO_MOCK: bool = True

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
