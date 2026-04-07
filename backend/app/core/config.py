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

    # --- 向量数据库 (Milvus/Qdrant) ---
    VECTOR_DB_HOST: str = "localhost"
    VECTOR_DB_PORT: int = 19530

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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
