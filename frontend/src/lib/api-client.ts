/** 统一 fetch 封装，自动处理 JSON 和错误 */
async function apiFetch<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`API ${res.status}: ${text}`)
  }
  return res.json() as Promise<T>
}

// ── Types ─────────────────────────────────────────────
export interface TraceRun {
  id: string
  created_at: string
  query: string
}

export interface TraceSpan {
  id: string
  agent_name: string
  operation: string
  duration_ms: number
  status: 'success' | 'error'
  started_at: string
  input?: string
  output?: string
}

export interface PromptTemplate {
  id: string
  name: string
  agent_type: string
  active_version: number
  updated_at: string
}

export interface PromptVersion {
  version: number
  content: string
  commit_msg: string
  created_at: string
}

// ── Chat API ──────────────────────────────────────────
export const chatApi = {
  /** 返回 ReadableStream，调用方自行消费 SSE */
  stream: async (query: string): Promise<ReadableStream<Uint8Array>> => {
    const res = await fetch('/api/chat/stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    })
    if (!res.body) throw new Error('No response body')
    return res.body
  },
}

// ── Trace API ─────────────────────────────────────────
export const traceApi = {
  listRuns: () => apiFetch<TraceRun[]>('/api/traces/runs'),
  getTrace: (runId: string) => apiFetch<TraceSpan[]>(`/api/traces/${runId}`),
}

// ── Prompt API ────────────────────────────────────────
export const promptApi = {
  listTemplates: () => apiFetch<PromptTemplate[]>('/api/prompts'),
  listVersions: (id: string) =>
    apiFetch<PromptVersion[]>(`/api/prompts/${id}/versions`),
  create: (data: Pick<PromptTemplate, 'name' | 'agent_type'> & { content: string }) =>
    apiFetch<PromptTemplate>('/api/prompts', { method: 'POST', body: JSON.stringify(data) }),
  activate: (id: string, version: number) =>
    apiFetch<void>(`/api/prompts/${id}/versions/${version}/activate`, { method: 'POST' }),
}
