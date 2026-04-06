import { NextResponse } from 'next/server'

const FASTAPI = process.env.FASTAPI_URL ?? 'http://localhost:8000'

/** GET /api/prompts/[id]/versions */
export async function GET(
  _req: Request,
  { params }: { params: { id: string } },
) {
  const res = await fetch(`${FASTAPI}/api/prompts/${params.id}/versions`)
  const data = await res.json()
  return NextResponse.json(data)
}
