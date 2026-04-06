# DaseS Frontend

基于 **Next.js 15 (App Router)** 构建的 CS 学术论文多智能体检索系统前端。

## 技术栈

| 层级 | 技术 |
|------|------|
| 框架 | Next.js 15 (App Router) + TypeScript |
| UI 系统 | Tailwind CSS + Shadcn UI + Lucide React |
| 异步状态 | TanStack Query (React Query v5) |
| Agent 可视化 | React Flow (`@xyflow/react` v12) |
| BFF 层 | Next.js API Routes → FastAPI |
| 测试 | Vitest (单元) + Playwright (E2E) |

## 目录结构

```
src/
├── app/
│   ├── (main)/                  # 路由组（共享侧边栏布局）
│   │   ├── chat/page.tsx        # AI 对话页
│   │   ├── trace/page.tsx       # Agent Trace 可视化
│   │   ├── prompts/page.tsx     # Prompt 版本管理
│   │   └── layout.tsx
│   ├── api/                     # BFF — 代理 FastAPI 请求
│   │   ├── chat/stream/route.ts # SSE 流式对话转发
│   │   ├── traces/              # Trace Runs / Spans
│   │   └── prompts/             # Prompt 模板 CRUD
│   ├── layout.tsx               # 根布局（注入 QueryProvider）
│   └── globals.css              # Tailwind + CSS 变量
├── components/
│   ├── chat/chat-interface.tsx  # 对话界面（SSE 流式渲染）
│   ├── trace/trace-flow.tsx     # React Flow 思维链图
│   ├── prompts/prompt-manager.tsx # 模板列表 + 版本管理
│   ├── layout/sidebar.tsx       # 可折叠侧边导航
│   └── providers.tsx            # TanStack QueryClientProvider
├── hooks/
│   ├── use-chat.ts              # SSE 流式对话 Hook
│   ├── use-trace.ts             # TanStack Query - Trace 数据
│   └── use-prompts.ts           # TanStack Query - Prompt CRUD
└── lib/
    ├── api-client.ts            # fetch 封装 + 所有类型定义
    └── utils.ts                 # shadcn cn() 工具函数
```

## 快速开始

```bash
# 安装依赖
npm install

# 启动开发服务器（http://localhost:3000）
npm run dev

# 生产构建
npm run build && npm start
```

> 开发前确保后端 FastAPI 服务已在 `http://localhost:8000` 运行，
> 或通过 `.env.local` 指定地址：
>
> ```env
> FASTAPI_URL=http://localhost:8000
> ```

## 页面说明

### `/chat` — AI 对话
- 与多智能体系统对话，检索 CS 学术论文
- 使用 SSE（Server-Sent Events）实现逐 token 流式输出
- 支持中断生成（Stop 按钮）

### `/trace` — Agent Trace 可视化
- 下拉选择历史 Run，查看 Agent 执行链路
- React Flow 画布展示节点（Coordinator → Retriever → Summarizer）
- 表格展示每个 Span 的耗时、状态与时间戳

### `/prompts` — Prompt 版本管理
- 管理各 Agent（coordinator / retriever / summarizer / analyzer）的 Prompt 模板
- 展开查看历史版本，支持预览内容与一键激活

## BFF 架构

```
浏览器
  └─ /api/chat/stream     (Next.js Route)  →  POST /api/chat/stream   (FastAPI)
  └─ /api/traces/runs     (Next.js Route)  →  GET  /api/traces/runs   (FastAPI)
  └─ /api/traces/[runId]  (Next.js Route)  →  GET  /api/traces/:id    (FastAPI)
  └─ /api/prompts         (Next.js Route)  →  GET/POST /api/prompts   (FastAPI)
```

所有后端调用经过 Next.js API Routes 中转，前端代码无需关心后端地址，也避免了 CORS 问题。

## 测试

```bash
# 单元测试（Vitest）
npm run test

# 监听模式
npm run test:watch

# E2E 测试（Playwright，需先启动 dev server）
npm run test:e2e
```

E2E 测试文件放在 `src/tests/e2e/` 目录下。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `FASTAPI_URL` | `http://localhost:8000` | FastAPI 后端地址 |
