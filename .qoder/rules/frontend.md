---
trigger: always_on
---
# DaseS Frontend Rules

## 技术栈规范
- **框架**: Next.js 15 (App Router)。
- **组件**: 使用 Radix UI + Tailwind CSS (Shadcn UI)。
- **状态管理**: 
  - 服务端状态: TanStack Query (React Query) v5。
  - 流式对话: 自定义 `use-chat` hook 处理 SSE。
- **可视化**: Agent Trace 使用 `@xyflow/react` (React Flow)。

## 代码风格
- **TypeScript**: 严禁使用 `any`。所有 API 响应必须有对应 interface。
- **组件声明**: 优先使用 Server Components；涉及交互、Hooks 时使用 `'use client'`。
- **Icons**: 统一使用 `lucide-react`。
- **UI 规范**: 遵循 `components.json` 中的配置，保持 Shadcn 主题一致性。

## API 请求
- 所有的 API 请求应通过 `lib/api-client.ts` 封装，支持统一的错误处理。
- BFF 模式: 页面组件 -> `app/api/` (Next.js Route) -> `FastAPI`。

## 性能优化
- 图片使用 `next/image`。
- 列表渲染必须带 `key`。
- 复杂的 Trace 图表使用动态导入 (Dynamic Import) 以减少首屏体积。