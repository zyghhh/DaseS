FROM ghcr.io/astral-sh/uv:python3.11-slim

WORKDIR /app

# 安装系统依赖 (PDF 解析需要)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 启用字节码编译加速启动
ENV UV_COMPILE_BYTECODE=1
# 使用 copy 模式避免挂载卷问题
ENV UV_LINK_MODE=copy

# 先安装依赖（利用 Docker 层缓存）
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# 复制源代码并安装项目本身
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
