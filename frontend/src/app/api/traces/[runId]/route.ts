import { NextResponse } from 'next/server'

const FASTAPI = process.env.FASTAPI_URL ?? 'http://localhost:8000'

/** GET /api/traces/[runId] — 获取单个 Run 的 Spans */
export async function GET(
  _req: Request,
  { params }: { params: { runId: string } },
) {
  const res = await fetch(`${FASTAPI}/api/traces/${params.runId}`, {
    cache: 'no-store',
  })
  const data = await res.json()
  return NextResponse.json(data)
}
