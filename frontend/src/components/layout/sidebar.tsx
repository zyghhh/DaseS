'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { MessageSquare, Network, FileCode2, ChevronLeft, ChevronRight } from 'lucide-react'
import { useState } from 'react'
import { cn } from '@/lib/utils'

const NAV_ITEMS = [
  { href: '/chat',    label: 'AI 对话',      icon: MessageSquare },
  { href: '/trace',   label: 'Agent Trace',  icon: Network },
  { href: '/prompts', label: 'Prompt 管理',  icon: FileCode2 },
]

export function Sidebar() {
  const pathname = usePathname()
  const [collapsed, setCollapsed] = useState(false)

  return (
    <aside
      className={cn(
        'flex flex-col border-r bg-slate-900 text-slate-100 transition-all duration-200',
        collapsed ? 'w-14' : 'w-52',
      )}
    >
      {/* Logo */}
      <div className="flex items-center gap-2 px-4 h-14 border-b border-slate-700">
        <span className="text-primary font-bold text-lg">DS</span>
        {!collapsed && (
          <span className="font-semibold text-sm tracking-wide">DaseS</span>
        )}
      </div>

      {/* 导航 */}
      <nav className="flex-1 py-4 space-y-1">
        {NAV_ITEMS.map(({ href, label, icon: Icon }) => {
          const active = pathname.startsWith(href)
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                'flex items-center gap-3 px-4 py-2.5 text-sm rounded-md mx-2 transition-colors',
                active
                  ? 'bg-primary text-white'
                  : 'text-slate-400 hover:bg-slate-800 hover:text-slate-100',
              )}
            >
              <Icon className="w-4 h-4 flex-shrink-0" />
              {!collapsed && <span>{label}</span>}
            </Link>
          )
        })}
      </nav>

      {/* 折叠按钮 */}
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="flex items-center justify-center h-10 border-t border-slate-700 text-slate-400 hover:text-slate-100"
      >
        {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
      </button>
    </aside>
  )
}
