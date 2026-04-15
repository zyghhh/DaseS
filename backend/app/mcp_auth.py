"""
MCP API Key 认证与每日配额管理。

Phase 4 目标：
- 通过 Authorization: Bearer <key> 保护 HTTP MCP 入口。
- 按 API Key 统计 UTC 自然日配额。
- 配额存储默认使用 mock/in-memory，部署 Redis 后可切换到 redis 后端。
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from starlette.responses import JSONResponse

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class APIKeyConfig:
    user: str
    quota: int


@dataclass(frozen=True)
class QuotaResult:
    allowed: bool
    used: int
    quota: int


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _seconds_until_utc_tomorrow() -> int:
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((tomorrow - now).total_seconds()))


def _parse_api_keys() -> dict[str, APIKeyConfig]:
    """从 MCP_API_KEYS 解析 API Key 配置。

    支持：
    - JSON: {"sk-xxx": {"user": "alice", "quota": 100}}
    - JSON: {"sk-xxx": 100}
    - 逗号分隔: sk-xxx,sk-yyy
    """
    raw = settings.MCP_API_KEYS.strip()
    if not raw:
        return {}

    keys: dict[str, APIKeyConfig] = {}
    if raw.startswith("{"):
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("MCP_API_KEYS JSON 解析失败，认证将拒绝所有请求: %s", exc)
            return {}

        for api_key, value in loaded.items():
            if not isinstance(api_key, str) or not api_key:
                continue
            if isinstance(value, dict):
                user = str(value.get("user") or api_key[:8])
                quota = int(value.get("quota") or settings.MCP_DEFAULT_QUOTA)
            else:
                user = api_key[:8]
                quota = int(value or settings.MCP_DEFAULT_QUOTA)
            keys[api_key] = APIKeyConfig(user=user, quota=max(0, quota))
        return keys

    for api_key in raw.split(","):
        api_key = api_key.strip()
        if api_key:
            keys[api_key] = APIKeyConfig(user=api_key[:8], quota=max(0, settings.MCP_DEFAULT_QUOTA))
    return keys


class QuotaStore:
    async def increment(self, api_key: str, quota: int) -> QuotaResult:
        raise NotImplementedError

    async def get_used(self, api_key: str) -> int:
        raise NotImplementedError

    async def close(self) -> None:
        return None


class MemoryQuotaStore(QuotaStore):
    """Redis 未部署前使用的 mock 配额服务。"""

    def __init__(self) -> None:
        self._usage: dict[str, dict[str, int | str]] = {}

    async def increment(self, api_key: str, quota: int) -> QuotaResult:
        today = _today_utc()
        usage = self._usage.get(api_key)

        if usage is None or usage.get("date") != today:
            used = 1
            self._usage[api_key] = {"date": today, "count": used}
        else:
            used = int(usage["count"]) + 1
            if used <= quota:
                usage["count"] = used

        return QuotaResult(allowed=used <= quota, used=used, quota=quota)

    async def get_used(self, api_key: str) -> int:
        usage = self._usage.get(api_key)
        if usage is None or usage.get("date") != _today_utc():
            return 0
        return int(usage["count"])


class RedisQuotaStore(QuotaStore):
    def __init__(self, redis_url: str, prefix: str) -> None:
        self._redis_url = redis_url
        self._prefix = prefix
        self._client: Any | None = None

    async def _get_client(self) -> Any:
        if self._client is None:
            from redis import asyncio as redis

            self._client = redis.from_url(self._redis_url, decode_responses=True)
        return self._client

    def _key(self, api_key: str) -> str:
        return f"{self._prefix}:{_today_utc()}:{api_key}"

    async def increment(self, api_key: str, quota: int) -> QuotaResult:
        client = await self._get_client()
        key = self._key(api_key)
        used = int(await client.incr(key))
        if used == 1:
            await client.expire(key, _seconds_until_utc_tomorrow())
        return QuotaResult(allowed=used <= quota, used=used, quota=quota)

    async def get_used(self, api_key: str) -> int:
        client = await self._get_client()
        value = await client.get(self._key(api_key))
        return int(value or 0)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


class FallbackQuotaStore(QuotaStore):
    def __init__(self, primary: QuotaStore, fallback: QuotaStore) -> None:
        self._primary = primary
        self._fallback = fallback
        self._using_fallback = False
        self._last_warning_at = 0.0

    def _warn(self, exc: Exception) -> None:
        now = time.monotonic()
        if now - self._last_warning_at > 60:
            logger.warning("Redis 配额服务不可用，临时降级到 mock/in-memory: %s", exc)
            self._last_warning_at = now

    async def increment(self, api_key: str, quota: int) -> QuotaResult:
        if self._using_fallback:
            return await self._fallback.increment(api_key, quota)
        try:
            return await self._primary.increment(api_key, quota)
        except Exception as exc:
            self._using_fallback = True
            self._warn(exc)
            return await self._fallback.increment(api_key, quota)

    async def get_used(self, api_key: str) -> int:
        if self._using_fallback:
            return await self._fallback.get_used(api_key)
        try:
            return await self._primary.get_used(api_key)
        except Exception as exc:
            self._using_fallback = True
            self._warn(exc)
            return await self._fallback.get_used(api_key)

    async def close(self) -> None:
        await self._primary.close()
        await self._fallback.close()


def build_quota_store() -> QuotaStore:
    backend = settings.MCP_QUOTA_BACKEND.lower()
    if backend in {"mock", "memory", "inmemory"}:
        return MemoryQuotaStore()
    if backend == "redis":
        redis_store = RedisQuotaStore(settings.REDIS_URL, settings.MCP_QUOTA_REDIS_PREFIX)
        if settings.MCP_REDIS_FALLBACK_TO_MOCK:
            return FallbackQuotaStore(redis_store, MemoryQuotaStore())
        return redis_store
    logger.warning("未知 MCP_QUOTA_BACKEND=%s，使用 mock/in-memory 配额存储", settings.MCP_QUOTA_BACKEND)
    return MemoryQuotaStore()


class MCPAuthMiddleware:
    """只保护 MCP_AUTH_PATH_PREFIX 下的 HTTP 请求。"""

    def __init__(self, app, quota_store: QuotaStore | None = None) -> None:
        self.app = app
        self.quota_store = quota_store or build_quota_store()
        self.api_keys = _parse_api_keys()

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if (
            not settings.MCP_AUTH_ENABLED
            or not path.startswith(settings.MCP_AUTH_PATH_PREFIX)
            or scope.get("method") == "OPTIONS"
        ):
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"").decode()
        if not auth_header.startswith("Bearer "):
            await self._send_error(
                scope,
                receive,
                send,
                401,
                "unauthorized",
                "Missing or invalid Authorization header. Use: Bearer <api-key>",
            )
            return

        api_key = auth_header[7:].strip()
        key_config = self.api_keys.get(api_key)
        if key_config is None:
            logger.warning("MCP 认证失败 [403] 无效 API Key: %s..., path=%s", api_key[:8], path)
            await self._send_error(scope, receive, send, 403, "forbidden", "Invalid API key")
            return

        quota_result = await self.quota_store.increment(api_key, key_config.quota)
        if not quota_result.allowed:
            logger.warning(
                "MCP 配额超限 [429] key=%s... used=%d quota=%d path=%s",
                api_key[:8],
                quota_result.used,
                quota_result.quota,
                path,
            )
            await self._send_error(
                scope,
                receive,
                send,
                429,
                "quota_exceeded",
                f"Daily quota exceeded ({quota_result.used}/{quota_result.quota})",
            )
            return

        scope["mcp_user"] = {
            "api_key": api_key,
            "user": key_config.user,
            "quota": quota_result.quota,
            "used": quota_result.used,
        }
        logger.info("MCP 认证通过: user=%s used=%d/%d path=%s", key_config.user, quota_result.used, quota_result.quota, path)
        await self.app(scope, receive, send)

    async def _send_error(self, scope, receive, send, status_code: int, error: str, message: str) -> None:
        response = JSONResponse({"error": error, "message": message}, status_code=status_code)
        await response(scope, receive, send)


async def close_quota_store(quota_store: QuotaStore | None) -> None:
    if quota_store is not None:
        await quota_store.close()
