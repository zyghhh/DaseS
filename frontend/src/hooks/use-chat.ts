'use client'

import { useCallback, useRef, useState } from 'react'
import { chatApi } from '@/lib/api-client'

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
}

export function useChat() {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const abortRef = useRef<AbortController | null>(null)

  const sendMessage = useCallback(async (query: string) => {
    // 追加用户消息
    const userMsg: ChatMessage = { id: crypto.randomUUID(), role: 'user', content: query }
    setMessages((prev) => [...prev, userMsg])

    // 创建 assistant 占位
    const assistantId = crypto.randomUUID()
    setMessages((prev) => [...prev, { id: assistantId, role: 'assistant', content: '' }])
    setIsStreaming(true)

    abortRef.current = new AbortController()
    try {
      const stream = await chatApi.stream(query)
      const reader = stream.getReader()
      const decoder = new TextDecoder()

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        const chunk = decoder.decode(value, { stream: true })
        for (const line of chunk.split('\n')) {
          if (line.startsWith('data: ')) {
            const token = line.slice(6)
            setMessages((prev) =>
              prev.map((m) =>
                m.id === assistantId ? { ...m, content: m.content + token } : m,
              ),
            )
          }
        }
      }
    } catch (err) {
      if ((err as Error).name !== 'AbortError') {
        console.error('[useChat]', err)
      }
    } finally {
      setIsStreaming(false)
    }
  }, [])

  const clearMessages = useCallback(() => setMessages([]), [])
  const abort = useCallback(() => abortRef.current?.abort(), [])

  return { messages, isStreaming, sendMessage, clearMessages, abort }
}
