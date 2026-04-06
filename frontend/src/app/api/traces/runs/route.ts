import { type NextRequest, NextResponse } from 'next/server'

const FASTAPI = process.env.FASTAPI_URL ?? 'http://localhost:8000'

/** GET /api/traces/runs — 获取所有 Run 列表 */
export async function GET() {
  const res = await fetch(`${FASTAPI}/api/traces/runs`, { cache: 'no-store' })
  const data = await res.json()
  return NextResponse.json(data)
}
