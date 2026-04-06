import { useQuery } from '@tanstack/react-query'
import { traceApi, type TraceSpan } from '@/lib/api-client'

export const traceKeys = {
  runs: ['traces', 'runs'] as const,
  trace: (runId: string) => ['traces', runId] as const,
}

export function useTraceRuns() {
  return useQuery({
    queryKey: traceKeys.runs,
    queryFn: () => traceApi.listRuns(),
  })
}

export function useTrace(runId: string | null) {
  return useQuery({
    queryKey: traceKeys.trace(runId ?? ''),
    queryFn: () => traceApi.getTrace(runId!),
    enabled: !!runId,
    select: (spans: TraceSpan[]) => ({
      spans,
      // 将 spans 转换为 React Flow nodes/edges
      nodes: spans.map((s, i) => ({
        id: s.id,
        type: 'default' as const,
        position: { x: i * 220, y: 80 },
        data: {
          label: `${s.agent_name}\n${s.operation}`,
          status: s.status,
          duration: s.duration_ms,
        },
      })),
      edges: spans.slice(1).map((s, i) => ({
        id: `e-${i}`,
        source: spans[i].id,
        target: s.id,
        animated: true,
        style: { stroke: spans[i].status === 'error' ? '#ef4444' : '#3b82f6' },
      })),
    }),
  })
}
