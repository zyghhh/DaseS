import { type NextRequest } from 'next/server'

const FASTAPI = process.env.FASTAPI_URL ?? 'http://localhost:8000'

/**
 * POST /api/chat/stream
 * 将 SSE 流请求透传给 FastAPI，保持流式响应
 */
export async function POST(req: NextRequest) {
  const body = await req.json()

  const upstream = await fetch(`${FASTAPI}/api/chat/stream`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })

  // 直接透传上游的 SSE 流
  return new Response(upstream.body, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    },
  })
}
