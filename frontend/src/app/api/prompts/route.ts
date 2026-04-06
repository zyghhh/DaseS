import { type NextRequest, NextResponse } from 'next/server'

const FASTAPI = process.env.FASTAPI_URL ?? 'http://localhost:8000'

/** GET /api/prompts — 获取所有 Prompt 模板 */
export async function GET() {
  const res = await fetch(`${FASTAPI}/api/prompts`)
  const data = await res.json()
  return NextResponse.json(data)
}

/** POST /api/prompts — 新建 Prompt 模板 */
export async function POST(req: NextRequest) {
  const body = await req.json()
  const res = await fetch(`${FASTAPI}/api/prompts`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const data = await res.json()
  return NextResponse.json(data, { status: res.status })
}
