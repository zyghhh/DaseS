'use client'

import { KeyboardEvent, useRef, useState } from 'react'
import { Send, Square, Trash2, Bot, User } from 'lucide-react'
import { useChat } from '@/hooks/use-chat'
import { cn } from '@/lib/utils'

export function ChatInterface() {
  const { messages, isStreaming, sendMessage, clearMessages, abort } = useChat()
  const [input, setInput] = useState('')
  const bottomRef = useRef<HTMLDivElement>(null)

  function handleSend() {
    const text = input.trim()
    if (!text || isStreaming) return
    setInput('')
    sendMessage(text).then(() => {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    })
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  return (
    <div className="flex flex-col h-[calc(100vh-96px)]">
      {/* 消息列表 */}
      <div className="flex-1 overflow-y-auto space-y-4 pb-4 pr-1">
        {messages.length === 0 && (
          <div className="flex flex-col items-center justify-center h-full text-muted-foreground gap-3">
            <Bot className="w-12 h-12 opacity-30" />
            <p className="text-sm">输入问题，开始检索 CS 学术论文</p>
          </div>
        )}

        {messages.map((msg) => (
          <div
            key={msg.id}
            className={cn('flex gap-3', msg.role === 'user' && 'flex-row-reverse')}
          >
            <div
              className={cn(
                'flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center text-white text-xs',
                msg.role === 'user' ? 'bg-primary' : 'bg-emerald-500',
              )}
            >
              {msg.role === 'user' ? <User className="w-4 h-4" /> : <Bot className="w-4 h-4" />}
            </div>
            <div
              className={cn(
                'max-w-2xl rounded-lg px-4 py-3 text-sm whitespace-pre-wrap',
                msg.role === 'user'
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-muted text-foreground',
              )}
            >
              {msg.content}
              {isStreaming && msg.role === 'assistant' && !msg.content && (
                <span className="inline-block w-2 h-4 bg-current animate-pulse ml-1" />
              )}
            </div>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      {/* 输入区 */}
      <div className="border-t pt-4 space-y-2">
        <div className="flex gap-2">
          <textarea
            className="flex-1 min-h-[80px] max-h-[160px] resize-none rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            placeholder="例如：推荐最近三年关于 RAG 优化的顶会论文…（Enter 发送，Shift+Enter 换行）"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={isStreaming}
          />
          <div className="flex flex-col gap-2">
            {isStreaming ? (
              <button
                onClick={abort}
                className="flex items-center gap-1 px-3 py-2 text-sm rounded-md bg-destructive text-white hover:bg-destructive/90"
              >
                <Square className="w-4 h-4" /> 停止
              </button>
            ) : (
              <button
                onClick={handleSend}
                disabled={!input.trim()}
                className="flex items-center gap-1 px-3 py-2 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                <Send className="w-4 h-4" /> 发送
              </button>
            )}
            <button
              onClick={clearMessages}
              className="flex items-center gap-1 px-3 py-2 text-sm rounded-md border hover:bg-muted"
            >
              <Trash2 className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
