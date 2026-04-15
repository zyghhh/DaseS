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
# cd /mnt/d/vDesktop/DaseS/backend & uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

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
from app.mcp_server import mcp as _mcp  # noqa: E402


class _MCPHostMiddleware:
    """MCP 安全修復中间件。

    mcp SDK ≥ 1.26 引入 DNS rebinding 防护，但存在 "late host binding" bug：
    FastMCP() 的 transport_security 参数没有正确传入安全检查层，
    导致 enable_dns_rebinding_protection=False 无效，远程 IP 访问就返回 421。
    修復方案：在请求进入 MCP ASGI 子应用前，将 Host 头改写为 localhost。
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] in ("http", "websocket"):
            scope = {
                **scope,
                "headers": [
                    (b"host", b"localhost") if k.lower() == b"host" else (k, v)
                    for k, v in scope.get("headers", [])
                ],
            }
        await self.app(scope, receive, send)


if settings.MCP_AUTH_ENABLED:
    mcp_app = MCPAuthMiddleware(_MCPHostMiddleware(get_streamable_http_app()))
else:
    mcp_app = _MCPHostMiddleware(get_streamable_http_app())

# Streamable HTTP transport（Claude Code / 支持新协议的客户端）
app.mount("/mcp", mcp_app)

# SSE transport（Gemini CLI 等使用旧协议的客户端）
app.mount("/sse", _MCPHostMiddleware(_mcp.sse_app()))


@app.get("/health")
async def health_check():
    return {"status": "ok", "version": settings.VERSION}
