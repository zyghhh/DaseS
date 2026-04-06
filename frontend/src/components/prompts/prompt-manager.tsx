'use client'

import { useState } from 'react'
import { Plus, ChevronDown, ChevronRight, Zap, Eye } from 'lucide-react'
import {
  usePromptTemplates,
  usePromptVersions,
  useCreatePrompt,
  useActivateVersion,
} from '@/hooks/use-prompts'
import { cn } from '@/lib/utils'

const AGENT_TYPES = ['coordinator', 'retriever', 'summarizer', 'analyzer']

export function PromptManager() {
  const { data: templates, isLoading } = usePromptTemplates()
  const createPrompt = useCreatePrompt()
  const activateVersion = useActivateVersion()

  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [previewContent, setPreviewContent] = useState<string | null>(null)
  const [form, setForm] = useState({ name: '', agent_type: 'coordinator', content: '' })

  const { data: versions } = usePromptVersions(expandedId)

  async function handleCreate() {
    await createPrompt.mutateAsync(form)
    setShowCreate(false)
    setForm({ name: '', agent_type: 'coordinator', content: '' })
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Prompt 版本管理</h2>
        <button
          onClick={() => setShowCreate(true)}
          className="flex items-center gap-1.5 px-3 py-2 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90"
        >
          <Plus className="w-4 h-4" /> 新建模板
        </button>
      </div>

      {/* 模板列表 */}
      <div className="border rounded-lg overflow-hidden divide-y">
        {isLoading && (
          <div className="px-4 py-8 text-center text-sm text-muted-foreground">加载中…</div>
        )}
        {templates?.map((tpl) => (
          <div key={tpl.id}>
            <button
              className="w-full flex items-center gap-3 px-4 py-3 text-sm hover:bg-muted/50 text-left"
              onClick={() => setExpandedId(expandedId === tpl.id ? null : tpl.id)}
            >
              {expandedId === tpl.id ? (
                <ChevronDown className="w-4 h-4 flex-shrink-0" />
              ) : (
                <ChevronRight className="w-4 h-4 flex-shrink-0" />
              )}
              <span className="font-medium flex-1">{tpl.name}</span>
              <span className="px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 text-xs">
                {tpl.agent_type}
              </span>
              <span className="text-muted-foreground text-xs">v{tpl.active_version}</span>
              <span className="text-muted-foreground text-xs">
                {new Date(tpl.updated_at).toLocaleDateString('zh-CN')}
              </span>
            </button>

            {/* 版本列表（展开） */}
            {expandedId === tpl.id && (
              <div className="bg-slate-50/50 border-t">
                {!versions ? (
                  <p className="px-8 py-3 text-xs text-muted-foreground">加载版本…</p>
                ) : (
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-muted-foreground">
                        {['版本', '提交说明', '创建时间', '操作'].map((h) => (
                          <th key={h} className="px-4 py-2 text-left font-medium">{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y">
                      {versions.map((ver) => (
                        <tr key={ver.version} className="hover:bg-white/60">
                          <td className="px-4 py-2">
                            <span className={cn(
                              'px-1.5 py-0.5 rounded text-xs font-mono',
                              ver.version === tpl.active_version
                                ? 'bg-green-100 text-green-700'
                                : 'bg-slate-100',
                            )}>
                              v{ver.version}
                              {ver.version === tpl.active_version && ' ✓'}
                            </span>
                          </td>
                          <td className="px-4 py-2 text-muted-foreground">{ver.commit_msg}</td>
                          <td className="px-4 py-2 text-muted-foreground">
                            {new Date(ver.created_at).toLocaleString('zh-CN')}
                          </td>
                          <td className="px-4 py-2">
                            <div className="flex gap-2">
                              <button
                                onClick={() => setPreviewContent(ver.content)}
                                className="flex items-center gap-1 px-2 py-1 rounded border text-xs hover:bg-muted"
                              >
                                <Eye className="w-3 h-3" /> 预览
                              </button>
                              <button
                                onClick={() =>
                                  activateVersion.mutate({ id: tpl.id, version: ver.version })
                                }
                                disabled={ver.version === tpl.active_version}
                                className="flex items-center gap-1 px-2 py-1 rounded bg-primary/10 text-primary text-xs hover:bg-primary/20 disabled:opacity-40"
                              >
                                <Zap className="w-3 h-3" /> 激活
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* 新建模板 Modal */}
      {showCreate && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded-xl shadow-xl w-[560px] p-6 space-y-4">
            <h3 className="font-semibold text-base">新建 Prompt 模板</h3>
            <div className="space-y-3">
              <label className="block">
                <span className="text-sm font-medium">模板名称</span>
                <input
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                  placeholder="e.g. paper_retrieval_system"
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                />
              </label>
              <label className="block">
                <span className="text-sm font-medium">Agent 类型</span>
                <select
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                  value={form.agent_type}
                  onChange={(e) => setForm({ ...form, agent_type: e.target.value })}
                >
                  {AGENT_TYPES.map((t) => (
                    <option key={t} value={t}>{t}</option>
                  ))}
                </select>
              </label>
              <label className="block">
                <span className="text-sm font-medium">Prompt 内容</span>
                <textarea
                  className="mt-1 w-full h-40 rounded-md border border-input bg-background px-3 py-2 text-sm font-mono resize-none focus:outline-none focus:ring-2 focus:ring-ring"
                  value={form.content}
                  onChange={(e) => setForm({ ...form, content: e.target.value })}
                />
              </label>
            </div>
            <div className="flex justify-end gap-2">
              <button
                onClick={() => setShowCreate(false)}
                className="px-4 py-2 text-sm rounded-md border hover:bg-muted"
              >
                取消
              </button>
              <button
                onClick={handleCreate}
                disabled={!form.name || !form.content || createPrompt.isPending}
                className="px-4 py-2 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                {createPrompt.isPending ? '创建中…' : '创建'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 预览 Drawer */}
      {previewContent !== null && (
        <div className="fixed inset-y-0 right-0 z-50 w-[540px] bg-white shadow-2xl flex flex-col">
          <div className="flex items-center justify-between px-6 py-4 border-b">
            <h3 className="font-semibold">Prompt 内容预览</h3>
            <button onClick={() => setPreviewContent(null)} className="text-muted-foreground hover:text-foreground">✕</button>
          </div>
          <pre className="flex-1 overflow-auto p-6 text-sm font-mono bg-slate-50 whitespace-pre-wrap">
            {previewContent}
          </pre>
        </div>
      )}
    </div>
  )
}
