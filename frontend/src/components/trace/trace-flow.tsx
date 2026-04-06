'use client'

import { useState } from 'react'
import { ReactFlow, Background, Controls, MiniMap, type NodeTypes } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { CheckCircle, XCircle, Clock } from 'lucide-react'
import { useTraceRuns, useTrace } from '@/hooks/use-trace'
import { cn } from '@/lib/utils'

/** 自定义 Agent 节点 */
function AgentNode({ data }: { data: { label: string; status: string; duration: number } }) {
  return (
    <div
      className={cn(
        'px-3 py-2 rounded-lg border-2 bg-white text-xs min-w-[120px] text-center shadow-sm',
        data.status === 'success' ? 'border-blue-400' : 'border-red-400',
      )}
    >
      <div className="font-semibold text-slate-700 whitespace-pre-line">{data.label}</div>
      <div className="flex items-center justify-center gap-1 mt-1 text-muted-foreground">
        {data.status === 'success' ? (
          <CheckCircle className="w-3 h-3 text-green-500" />
        ) : (
          <XCircle className="w-3 h-3 text-red-500" />
        )}
        <Clock className="w-3 h-3" />
        <span>{data.duration}ms</span>
      </div>
    </div>
  )
}

const nodeTypes: NodeTypes = { default: AgentNode }

export function TraceFlow() {
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const { data: runs, isLoading: runsLoading } = useTraceRuns()
  const { data: traceData, isLoading: traceLoading } = useTrace(selectedRunId)

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Agent 执行链路</h2>
        <select
          className="h-9 rounded-md border border-input bg-background px-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring w-72"
          value={selectedRunId ?? ''}
          onChange={(e) => setSelectedRunId(e.target.value || null)}
          disabled={runsLoading}
        >
          <option value="">选择 Run ID…</option>
          {runs?.map((r) => (
            <option key={r.id} value={r.id}>
              {r.id.slice(0, 12)}… — {new Date(r.created_at).toLocaleString('zh-CN')}
            </option>
          ))}
        </select>
      </div>

      {/* React Flow 画布 */}
      <div className="border rounded-lg overflow-hidden bg-slate-50" style={{ height: 420 }}>
        {traceLoading ? (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            加载中…
          </div>
        ) : traceData?.nodes.length ? (
          <ReactFlow
            nodes={traceData.nodes}
            edges={traceData.edges}
            nodeTypes={nodeTypes}
            fitView
          >
            <Background />
            <Controls />
            <MiniMap />
          </ReactFlow>
        ) : (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            请选择一个 Run 查看 Trace
          </div>
        )}
      </div>

      {/* Span 详情表格 */}
      {traceData?.spans && traceData.spans.length > 0 && (
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-muted">
              <tr>
                {['Agent', '操作', '耗时 (ms)', '状态', '开始时间'].map((h) => (
                  <th key={h} className="px-4 py-2 text-left font-medium text-muted-foreground">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {traceData.spans.map((s, i) => (
                <tr key={s.id} className={cn('border-t', i % 2 === 0 ? 'bg-white' : 'bg-slate-50/50')}>
                  <td className="px-4 py-2 font-medium">{s.agent_name}</td>
                  <td className="px-4 py-2 text-muted-foreground">{s.operation}</td>
                  <td className="px-4 py-2">{s.duration_ms}</td>
                  <td className="px-4 py-2">
                    <span className={cn(
                      'inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium',
                      s.status === 'success'
                        ? 'bg-green-100 text-green-700'
                        : 'bg-red-100 text-red-700',
                    )}>
                      {s.status}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-muted-foreground">
                    {new Date(s.started_at).toLocaleTimeString('zh-CN')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
