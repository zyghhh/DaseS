"""
DaseS - 基于CS学术论文的多智能体检索系统
FastAPI 应用入口
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.services.es_service import close_es_client, ensure_index


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理：启动/关闭时的资源初始化与清理"""
    # 初始化 Elasticsearch 内引
    await ensure_index()
    yield
    # 关闭 ES 连接
    await close_es_client()


app = FastAPI(
    title=settings.PROJECT_NAME,
    description="基于计算机科学领域学术论文的多智能体检索系统",
    version=settings.VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- 路由注册 ---
from app.api import search  # noqa: E402

app.include_router(search.router, prefix="/api/v1/search", tags=["Search"])
# app.include_router(chat.router, prefix="/api/v1/chat", tags=["Chat"])
# app.include_router(agents.router, prefix="/api/v1/agents", tags=["Agents"])
# app.include_router(prompts.router, prefix="/api/v1/prompts", tags=["Prompts"])
# app.include_router(traces.router, prefix="/api/v1/traces", tags=["Traces"])


@app.get("/health")
async def health_check():
    return {"status": "ok", "version": settings.VERSION}
