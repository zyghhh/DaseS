# DaseS - 基于CS学术论文的多智能体检索系统

## 项目概述

面向计算机科学领域学术论文的多智能体检索系统（数据 + 服务）。后端基于 Python + FastAPI 提供学术论文检索 API 服务，前端支持 Agent Trace 监控、Prompt 版本化管理、用户与智能体直接对话。

## 项目结构

```
DaseS/
├── backend/                    # 后端服务 (Python + FastAPI)
│   ├── app/
│   │   ├── api/                # API 路由层
│   │   │   ├── chat.py         # 对话接口 (SSE 流式)
│   │   │   ├── search.py       # 论文检索接口
│   │   │   ├── agents.py       # 智能体管理接口
│   │   │   ├── prompts.py      # Prompt 版本化管理接口
│   │   │   └── traces.py       # Agent Trace 可观测性接口
│   │   ├── agents/             # LangGraph 智能体定义
│   │   │   ├── coordinator.py  # 协调器 - 意图识别与任务分发
│   │   │   ├── retriever.py    # 论文检索智能体
│   │   │   ├── summarizer.py   # 摘要生成智能体
│   │   │   └── analyzer.py     # 深度分析智能体
│   │   ├── core/               # 核心配置与依赖
│   │   │   ├── config.py       # 应用配置 (pydantic-settings)
│   │   │   └── deps.py         # 依赖注入 (DB, Redis)
│   │   ├── models/             # 数据模型 (SQLAlchemy)
│   │   │   └── schemas.py      # Paper, PromptTemplate, PromptVersion
│   │   ├── services/           # 业务逻辑层
│   │   │   └── prompt_service.py  # Prompt 版本管理服务
│   │   ├── rag/                # RAG 管线
│   │   │   ├── parser.py       # PDF 论文解析 (PyMuPDF + Unstructured)
│   │   │   ├── chunker.py      # 语义分块 (按章节/段落)
│   │   │   └── retriever.py    # 混合检索 (向量 + BM25 + RRF)
│   │   └── main.py             # FastAPI 应用入口
│   ├── tests/                  # 测试目录
│   ├── pyproject.toml          # Python 依赖管理
│   ├── Dockerfile
│   └── .env.example            # 环境变量模板
├── frontend/                   # 前端 (Next.js 15 + TypeScript)
│   ├── src/
│   │   ├── app/
│   │   │   ├── (main)/             # 路由组（含侧边栏布局）
│   │   │   │   ├── chat/page.tsx   # AI 对话页
│   │   │   │   ├── trace/page.tsx  # Agent Trace 可视化
│   │   │   │   ├── prompts/page.tsx# Prompt 版本管理
│   │   │   │   └── layout.tsx
│   │   │   ├── api/                # BFF API Routes
│   │   │   │   ├── chat/stream/route.ts
│   │   │   │   ├── traces/[runId]/route.ts
│   │   │   │   └── prompts/route.ts
│   │   │   ├── layout.tsx          # 根布局（QueryProvider）
│   │   │   └── globals.css
│   │   ├── components/
│   │   │   ├── chat/chat-interface.tsx
│   │   │   ├── trace/trace-flow.tsx
│   │   │   ├── prompts/prompt-manager.tsx
│   │   │   ├── layout/sidebar.tsx
│   │   │   └── providers.tsx
│   │   ├── hooks/
│   │   │   ├── use-chat.ts         # SSE 流式
│   │   │   ├── use-trace.ts        # TanStack Query
│   │   │   └── use-prompts.ts      # TanStack Query
│   │   ├── lib/
│   │   │   ├── api-client.ts       # fetch 封装 + 类型定义
│   │   │   └── utils.ts            # shadcn cn()
│   │   └── tests/
│   │       ├── setup.ts
│   │       └── e2e/                # Playwright E2E
│   ├── next.config.ts
│   ├── tailwind.config.ts
│   ├── components.json             # shadcn/ui 配置
│   ├── vitest.config.ts
│   ├── playwright.config.ts
│   └── package.json
├── data/                       # 数据目录
│   ├── raw/                    # 原始论文 (PDF)
│   ├── processed/              # 处理后数据
│   └── scripts/
│       └── ingest.py           # 数据采集脚本
├── docker-compose.yml          # 基础设施编排
└── .gitignore
```

## 技术栈

### 后端
| 组件 | 技术 |
|------|------|
| Web 框架 | FastAPI |
| 多智能体编排 | LangGraph |
| LLM 调用 | LangChain + LiteLLM |
| 向量数据库 | Milvus |
| 关系数据库 | PostgreSQL + SQLAlchemy (async) |
| 缓存 | Redis |
| Embedding | BAAI/bge-large-en-v1.5 |
| 文档解析 | PyMuPDF + Unstructured |
| 关键词检索 | rank-bm25 |
| 可观测性 | LangFuse |

### 前端
| 组件 | 技术 |
|------|------|
| 框架 | Next.js 15 (App Router) + TypeScript |
| UI 系统 | Tailwind CSS + Shadcn UI + Lucide React |
| 异步状态 | TanStack Query (React Query v5) |
| Agent 可视化 | React Flow (@xyflow/react v12) |
| BFF 层 | Next.js API Routes → FastAPI (Python) |
| 测试 | Vitest (单元) + Playwright (E2E) |

### 基础设施
| 服务 | 端口 |
|------|------|
| 后端 API | 8000 |
| PostgreSQL | 5432 |
| Redis | 6379 |
| Milvus | 19530 |
| LangFuse | 3001 |
| 前端 (dev) | 3000 |

## 智能体架构

```
用户输入 → Coordinator(意图识别)
    ├→ PaperRetriever(=论文检索: 向量 + BM25 混合)
      #TODO   
      {命令行交互：
      1.用户输入 /search_semantic_articles user query
      2.cli 工具根据query提出几个针对性问题（明确/消歧问题），让用户先选择明确 
      3.cli工具获取回答后调用我的服务完成检索 并格式化输出检索结果
      }
    ├→ Summarizer(摘要生成: 单篇/多篇)
    └→ Analyzer(深度分析: 方法论对比/趋势分析)
    → Coordinator(结果汇总) → 用户 
```

## 数据源

| 来源 | 用途 |
|------|------|
| Semantic Scholar API | CS 论文元数据 + 全文 |
| arXiv API / Bulk | CS 预印本 |
| DBLP | 论文元数据 |
| OpenAlex | 开放学术图谱 |

## 快速开始

```bash
# 0. 安装 uv（若未安装）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 1. 启动基础设施
docker compose up -d postgres redis milvus langfuse

# 2. 后端开发
cd backend
cp .env.example .env          # 编辑 .env 填入实际配置
uv sync --extra dev           # 创建虚拟环境并安装全部依赖（含开发依赖）
uv run uvicorn app.main:app --reload

# 3. 前端开发
cd frontend
npm install
npm run dev
```

> **常用 uv 命令**
> - `uv sync` — 按 `uv.lock` 同步依赖
> - `uv add <pkg>` — 添加新依赖并更新锁文件
> - `uv run <cmd>` — 在项目虚拟环境中运行命令
> - `uv lock --upgrade` — 升级所有依赖到最新版本
