---
trigger: always_on
---
# DaseS Backend Rules

## 技术栈规范
- **包管理**: 必须使用 `uv`。添加依赖使用 `uv add`，运行使用 `uv run`。
- **框架**: FastAPI (异步模式)。所有路由函数必须使用 `async def`。
- **AI 编排**: LangGraph。Agent 逻辑必须定义在 `app/agents/` 目录下，使用 `TypedDict` 定义 State。
- **数据库**: 使用 SQLAlchemy 2.0 异步模式。
- **模型**: 使用 Pydantic v2 进行数据验证。

## 代码风格
- **类型提示**: 所有函数必须有完整的类型标注 (Type Hints)。
- **Docstrings**: 使用 Google 风格的 Docstrings。中文注释。
- **错误处理**: 使用 FastAPI 的 `HTTPException` 并返回标准化的 JSON 错误响应。
- **响应格式**: 统一使用 `app/models/schemas.py` 中定义的 Response 模型。


## Code Style Standard
- **Architecture**: Use Clean Architecture (Separation of Concerns).
- **TypeScript**: Strict mode, no `any`, use `interface` over `type`.
- **Naming**: Descriptive names. Prefix boolean with `is`, `has`, `can`.
- **Functions**: Single Responsibility Principle. Max 25 lines per function.

## LangGraph 节点规范
- 每个节点函数必须接收 `State` 并返回 `dict` 以更新状态。
- 禁止在节点内直接编写复杂的业务逻辑，应调用 `app/services/` 中的服务。
- 关键节点必须包含 `langfuse` 的 trace 埋点。

## 目录索引
- API 路由: `app/api/`
- Agent 逻辑: `app/agents/`
- RAG 核心: `app/rag/`