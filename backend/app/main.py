"""
DaseS - 基于CS学术论文的多智能体检索系统
FastAPI 应用入口
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.mcp_server import mcp
from app.services.es_service import close_es_client, ensure_index
# cd /mnt/d/vDesktop/DaseS/backend
#.venv/bin/python -m uvicorn app.main:app --reload --port 8000

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理：启动/关闭时的资源初始化与清理"""
    # 初始化 Elasticsearch 索引
    await ensure_index()
    # 启动 MCP StreamableHTTP session manager（mount 不会自动触发子应用 lifespan）
    async with mcp.session_manager.run():
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


# --- MCP Server ---
from app.mcp_server import get_streamable_http_app  # noqa: E402
from app.mcp_auth import MCPAuthMiddleware  # noqa: E402

if settings.MCP_AUTH_ENABLED:
    # 认证模式：MCP 子应用包裹在认证中间件内
    mcp_app = MCPAuthMiddleware(get_streamable_http_app())
else:
    mcp_app = get_streamable_http_app()

# Streamable HTTP transport（Claude Code / 支持新协议的客户端）
app.mount("/mcp", mcp_app)


@app.get("/health")
async def health_check():
    return {"status": "ok", "version": settings.VERSION}
