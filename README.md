# DaseS — CS 学术论文多智能体检索系统

面向计算机科学领域的学术论文检索与相关工作生成系统。后端基于 Python + FastAPI，通过 LangGraph 编排多智能体协作完成论文检索、摘要与深度分析；同时暴露 MCP 服务，支持 Claude Code、Gemini CLI 等工具通过标准化命令直接调用。

---

## 目录

- [智能体架构](#智能体架构)
- [MCP 服务](#mcp-服务)
- [技术栈](#技术栈)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [数据源](#数据源)
- [基础设施](#基础设施)

---

## 智能体架构

系统提供两种使用入口，共享同一套后端智能体服务。

### web 端工作流

```
用户输入
    └─→ Coordinator（意图识别 & 任务分发）
            ├─→ PaperRetriever（论文检索：ES BM25 + 向量混合）
            ├─→ Summarizer（结构化摘要生成）
            └─→ Analyzer（深度分析：相关工作 / 调研报告）
    └─→ Coordinator（聚合各智能体结果）
    └─→ 用户（SSE 流式输出）
```

### cli 端工作流（MCP）

```
1. 用户输入： /search_semantic_articles <query>
2. cli 调用 start_search，根据 ES 分布分析返回消歧问题（会议方向 / 时间段 / CCF 评级）
3. 用户选择后，cli 携带 answers 调用 execute_search 完成检索，格式化输出论文列表
4. 多轮追问：用户继续输入 "基于这些文章写一篇相关工作"
5. cli 调用 write_related_work，按 venue 分组输出结构化引用列表，LLM 据此撰写段落
```

---

## 智能体职责

### Coordinator — 协调器

**文件**：`backend/app/agents/coordinator.py`

- 解析用户意图，判断本次请求属于「检索」「摘要」「深度分析」或「混合」任务
- 通过 LangGraph `StateGraph` 将任务分发到对应的专职智能体
- 收集各智能体输出，合并成最终回复后返回给用户
- 负责多轮对话上下文管理

### PaperRetriever — 论文检索智能体

**文件**：`backend/app/agents/retriever.py`

- 向量相似度检索（Elasticsearch 8.x kNN，BAAI/bge-large-en-v1.5 Embedding）
- BM25 关键词检索（Elasticsearch，定制 `paper_bm25` 相似度参数 k1=1.2 / b=0.5）
- 结果融合排序（Reciprocal Rank Fusion）
- 按需调用 Semantic Scholar / arXiv 在线 API 补充外部论文

### Summarizer — 摘要智能体

**文件**：`backend/app/agents/summarizer.py`

- 单篇论文摘要提炼（标题、方法、贡献、局限）
- 多篇论文综合摘要（主题归纳、研究趋势）
- 关键发现与技术细节提取

### Analyzer — 深度分析智能体

**文件**：`backend/app/agents/analyzer.py`

- 相关工作段落生成（支持 academic / survey / brief 三种风格）

---

## MCP 服务

MCP 服务以 **Streamable HTTP transport** 内嵌于 FastAPI，随后端服务一同启动，无需单独运行。
后端启动后，MCP 端点即可访问：`http://localhost:8000/mcp/`

> **实现说明**：`mcp_server.py` 中通过 `FastMCP.streamable_http_app()` 生成 ASGI 子应用，
> 并在 `main.py` lifespan 中显式调用 `mcp.session_manager.run()` 初始化 task group，
> 最终以 `app.mount("/mcp", mcp_app)` 挂载。

### 工具列表

| 工具 | 说明 |
|------|------|
| `start_search(query)` | 初始化检索会话，基于 ES aggregation 分析论文分布，返回消歧问题（会议方向 / 时间段 / CCF 评级）与 `session_id` |
| `execute_search(session_id, answers, search_mode, size, page)` | 根据用户对消歧问题的选择执行检索，结果追加到会话工作区；支持翻页；`search_mode` 可选 `bm25` / `phrase` / `fuzzy` |
| `list_session_papers(session_id)` | 查看当前会话工作区中已收集的全部论文 |
| `write_related_work(session_id, topic, dblp_keys, style)` | 将工作区论文按 venue 分组，输出结构化引用列表供 LLM 撰写相关工作段落 |

### 会话机制

- 每次 `start_search` 生成唯一 `session_id`（12位 hex），后续工具调用均需携带
- 会话状态存储于进程内存，TTL 为 1 小时（生产环境迁移 Redis）
- 同一会话内 `execute_search` 可多次调用（翻页 / 追加检索），结果自动去重

### 认证与配额

通过 `MCPAuthMiddleware` 保护 HTTP MCP 入口（`mcp_auth.py`）：

- `Authorization: Bearer <api-key>` 请求头认证
- 按 UTC 自然日统计每个 key 的调用配额
- 配额后端支持 `memory`（开发）/ `redis`（生产），Redis 不可用时自动降级

---

## 技术栈

### 后端

| 组件 | 技术 |
|------|------|
| Web 框架 | FastAPI |
| 多智能体编排 | LangGraph |
| LLM 调用 | LangChain + LiteLLM |
| 关键词检索 | Elasticsearch 8.x（BM25） |
| 向量检索 | Elasticsearch 8.x（kNN，dense_vector） |
| 关系数据库 | PostgreSQL + SQLAlchemy (async) |
| 缓存 / 配额 | Redis |
| Embedding | BAAI/bge-large-en-v1.5 |
| 文档解析 | PyMuPDF + Unstructured |
| MCP 服务 | FastMCP (Streamable HTTP，内嵌 FastAPI `/mcp/`) |
| 可观测性 | LangFuse |

### 前端

| 组件 | 技术 |
|------|------|
| 框架 | Next.js 15 (App Router) + TypeScript |
| UI 系统 | Tailwind CSS + Shadcn UI + Lucide React |
| 异步状态 | TanStack Query (React Query v5) |
| Agent 可视化 | React Flow (@xyflow/react v12) |
| BFF 层 | Next.js API Routes → FastAPI |
| 测试 | Vitest (单元) + Playwright (E2E) |

---

## 项目结构

```
DaseS/
├── backend/                    # 后端服务 (Python + FastAPI)
│   ├── app/
│   │   ├── agents/             # LangGraph 智能体定义
│   │   │   ├── coordinator.py  # 协调器：意图识别 & 任务分发
│   │   │   ├── retriever.py    # 论文检索（向量 + BM25 融合）
│   │   │   ├── summarizer.py   # 摘要生成（单篇 / 多篇）
│   │   │   └── analyzer.py     # 深度分析（对比 / 趋势 / 引用图谱）
│   │   ├── api/                # FastAPI 路由层
│   │   │   ├── chat.py         # SSE 流式对话接口
│   │   │   ├── search.py       # 论文检索接口
│   │   │   ├── agents.py       # 智能体管理接口
│   │   │   ├── prompts.py      # Prompt 版本化管理接口
│   │   │   └── traces.py       # Agent Trace 可观测性接口
│   │   ├── services/
│   │   │   ├── es_service.py   # Elasticsearch 检索 & 消歧服务
│   │   │   └── prompt_service.py  # Prompt 版本管理服务
│   │   ├── rag/                # RAG 管线
│   │   │   ├── parser.py       # PDF 论文解析
│   │   │   ├── chunker.py      # 语义分块（按章节/段落）
│   │   │   └── retriever.py    # 混合检索（向量 + BM25 + RRF）
│   │   ├── core/
│   │   │   ├── config.py       # pydantic-settings 配置
│   │   │   └── deps.py         # 依赖注入
│   │   ├── models/schemas.py   # Paper / PromptTemplate / PromptVersion
│   │   ├── mcp_server.py       # MCP 工具定义（Streamable HTTP，挂载于 /mcp）
│   │   ├── mcp_auth.py         # MCP API Key 认证 & 配额管理
│   │   └── main.py             # FastAPI 应用入口
│   ├── .env.example
│   ├── pyproject.toml
│   └── Dockerfile
├── frontend/                   # 前端 (Next.js 15)
│   └── src/
│       ├── app/(main)/
│       │   ├── chat/           # AI 对话页（SSE 流式）
│       │   ├── trace/          # Agent Trace 可视化
│       │   └── prompts/        # Prompt 版本管理
│       ├── components/         # UI 组件
│       ├── hooks/              # use-chat / use-trace / use-prompts
│       └── lib/                # api-client / utils
├── data/
│   ├── raw/                    # 原始论文数据
│   ├── processed/              # Neo4j / ES 导入数据
│   └── scripts/                # 数据采集与处理脚本
└── docker-compose.yml
```

---

## 快速开始

### 前提条件

- Docker & Docker Compose
- Python 3.11+，[uv](https://docs.astral.sh/uv/) 包管理器
- Node.js 20+

### 1. 启动基础设施

```bash
docker compose up -d postgres redis langfuse
```

Elasticsearch 需单独部署（或使用已有实例），在 `.env` 中配置 `ES_HOST`。

### 2. 后端

```bash
cd backend
cp .env.example .env               # 填入 LLM_API_KEY、ES_HOST 等实际值
uv sync --extra dev                 # 安装全部依赖（含开发依赖）
.venv/bin/python -m uvicorn app.main:app --reload
# API 文档：http://localhost:8000/docs
# MCP 端点：http://localhost:8000/mcp/
```

> `uv run` 需要 uv 在 `$PATH` 中。若系统未安装 uv，直接使用 `.venv/bin/python` 运行。

### 3. 前端

```bash
cd frontend
npm install
npm run dev
# 访问：http://localhost:3000
```

### 4. MCP 服务（cli 工具接入）

MCP 服务随后端 FastAPI 自动启动，无需额外操作。使用以下命令将 MCP 接入 CLI 工具。

#### Claude Code

Claude Code 的 MCP 配置通过 `claude mcp` 命令管理，实际写入 `~/.claude.json`。

```bash
# ── 添加 ──────────────────────────────────────────────
# 本地 HTTP（需 uvicorn 运行在 :8000）
claude mcp add --transport http --scope local dases http://localhost:8000/mcp/

# 远程 HTTP（替换为实际服务器 IP）
claude mcp add --transport http --scope local dases-remote http://<SERVER_IP>:8000/mcp/

# 本地 stdio（不依赖 uvicorn，直接启动进程）
claude mcp add --transport stdio --scope local dases-local -- \
  bash -c "cd /path/to/DaseS/backend && exec uv run python -m app.mcp_server"

# ── 查看 ──────────────────────────────────────────────
claude mcp list                  # 列出所有已配置的 MCP 服务及连接状态
claude mcp get <name>            # 查看某个服务的详细配置

# ── 删除 ──────────────────────────────────────────────
claude mcp remove <name>                        # 删除 local scope 的服务
claude mcp remove --scope user <name>           # 删除 user scope 的服务
claude mcp remove --scope project <name>        # 删除 project scope 的服务
```

> **scope 说明**
> - `--scope local`（默认）：写入 `~/.claude.json`，仅当前项目可见，不提交 git
> - `--scope project`：写入 `.claude/settings.json`，团队共享，可提交 git
> - `--scope user`：写入 `~/.claude.json` 全局段，所有项目可见
>
> **注意**：手动编辑 `.claude/settings.local.json` 对 MCP 配置**无效**，仅 `permissions` 等字段从该文件读取。

配置完成后，在 Claude Code 中即可使用：
```
/search_semantic_articles graph neural network
```

#### Gemini CLI

在项目根目录的 `.gemini/settings.json` 中配置：

```json
{
  "mcpServers": {
    "dases": {
      "httpUrl": "http://localhost:8000/mcp/"
    }
  }
}
```

---

## 常用 uv 命令

```bash
uv sync                  # 按 uv.lock 同步依赖
uv add <pkg>             # 添加新依赖并更新锁文件
uv run <cmd>             # 在项目虚拟环境中运行命令
uv lock --upgrade        # 升级所有依赖到最新版本
```

---

## 数据源

| 来源 | 用途 |
|------|------|
| DBLP | 论文元数据（标题 / 作者 / 会议 / 年份） |
| Semantic Scholar API | 论文摘要补全 |
| arXiv API / Bulk | CS 预印本全文 |
| OpenAlex | 开放学术图谱（引用关系） |
| CCF 目录 PDF | 会议 / 期刊 CCF 评级 |

---

## 基础设施端口

| 服务 | 端口 | 说明 |
|------|------|------|
| 后端 API | 8000 | FastAPI，`/docs` 查看接口文档 |
| 前端 | 3000 | Next.js dev server |
| PostgreSQL | 5432 | 关系数据库（Prompt 版本 / 用户数据） |
| Redis | 6379 | 缓存 & MCP 配额 |
| Elasticsearch | 9200 | BM25 关键词检索 & kNN 向量检索 & 消歧 aggregation |
| LangFuse | 3001 | Agent Trace 可观测性平台 |
