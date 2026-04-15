# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DaseS is a multi-agent retrieval system for CS academic papers. It features a Python FastAPI backend with LangGraph agents and a Next.js 15 frontend.

## Development Commands

### Backend (Python)

```bash
cd backend

# Install dependencies (requires uv)
uv sync --extra dev

# Run development server
# If uv is not in PATH, use .venv/bin/python directly:
.venv/bin/python -m uvicorn app.main:app --reload
# Alternatively with uv (requires uv in PATH):
# uv run uvicorn app.main:app --reload

# Run tests
uv run pytest
uv run pytest tests/test_file.py::test_name  # single test

# Lint and format
uv run ruff check .
uv run ruff format .

# Type check
uv run mypy app
```

### Frontend (TypeScript/Next.js)

```bash
cd frontend

# Install dependencies
npm install

# Run development server (http://localhost:3000)
npm run dev

# Build for production
npm run build

# Lint
npm run lint

# Unit tests (Vitest)
npm run test
npm run test:watch

# E2E tests (Playwright)
npm run test:e2e
```

### Infrastructure

```bash
# Start all services (PostgreSQL, Redis, Elasticsearch, LangFuse)
docker compose up -d

# Start specific services
docker compose up -d postgres redis elasticsearch langfuse
```

## MCP Server

The MCP server is embedded in FastAPI via **Streamable HTTP transport**. It starts automatically with the backend — no separate process needed.

**MCP endpoint**: `http://localhost:8000/mcp/`

**How it works**: `mcp_server.py` defines 4 tools via `FastMCP`. In `main.py`, the lifespan explicitly calls `mcp.session_manager.run()` to initialize the task group (required because `app.mount()` does not trigger sub-app lifespans). The app is mounted at `/mcp`.

**Claude Code configuration** — `.claude/settings.local.json` (project-level) or `~/.claude/settings.json` (global):

```json
{
  "mcpServers": {
    "dases": {
      "url": "http://localhost:8000/mcp/"
    }
  }
}
```

The project already has `.claude/settings.local.json` configured with the correct URL and pre-approved tool permissions.

**Available MCP tools**:
- `start_search(query)` — Initialize session, returns disambiguation questions + `session_id`
- `execute_search(session_id, answers?, search_mode?, size?, page?)` — Run search
- `list_session_papers(session_id)` — List papers in current session
- `write_related_work(session_id, topic, dblp_keys?, style?)` — Format papers for related work

**Slash Command**: `/search_semantic_articles <query>` (defined in `.claude/commands/search_semantic_articles.md`)

## Architecture

### Backend Multi-Agent System

```
User Query → Coordinator (intent classification)
    ├→ Retriever (paper search: vector + BM25 hybrid)
    ├→ Summarizer (single/multi-paper summaries)
    └→ Analyzer (method comparison, trend analysis)
    → Coordinator (result aggregation) → User
```

The agents are defined in `backend/app/agents/`. LangGraph orchestrates the state machine. Currently scaffolded - implementation pending.

### RAG Pipeline (`backend/app/rag/`)

- **parser.py**: PDF parsing via PyMuPDF + Unstructured
- **chunker.py**: Semantic chunking by sections/paragraphs
- **retriever.py**: Hybrid retrieval (vector similarity + BM25) with RRF fusion

### Search API

Papers are indexed in Elasticsearch with BM25 scoring. The search endpoint (`/api/v1/search/papers`) supports:
- `search_mode`: `bm25` (default), `phrase`, `fuzzy`
- Filters: `title`, `author`, `year_from`, `year_to`, `venue`

### Frontend BFF Pattern

All backend calls go through Next.js API Routes (`frontend/src/app/api/`):
```
Browser → /api/chat/stream (Next.js) → /api/v1/chat/stream (FastAPI)
```

This avoids CORS issues and keeps backend URLs out of client code.

### Key Data Flow

- **Chat**: SSE streaming from FastAPI through Next.js route to `useChat` hook
- **Traces**: TanStack Query fetches from `/api/traces/runs` and `/api/traces/[runId]`
- **Prompts**: TanStack Query with mutations for CRUD operations

## Code Conventions

### Frontend (from `.qoder/rules/frontend.md`)

- Use TanStack Query v5 for server state; custom hooks (`use-chat.ts`) for SSE streaming
- Agent Trace visualization uses `@xyflow/react` v12
- All API responses must have TypeScript interfaces (no `any`)
- Icons from `lucide-react` only
- Prefer Server Components; use `'use client'` only for interactive components
- Complex visualizations (React Flow) should use dynamic imports

### Backend

- Pydantic v2 for all data models (`backend/app/models/schemas.py`)
- Settings via `pydantic-settings` (`backend/app/core/config.py`)
- Async throughout: SQLAlchemy async, Elasticsearch async client
- BM25 parameters tuned for academic titles (b=0.5, k1=1.2)

## Environment

Backend requires `.env` file (copy from `backend/.env.example`):
- `DATABASE_URL`, `REDIS_URL` for relational DB and cache
- `ES_KNN_DIMS`, `ES_VECTOR_FIELD` for Elasticsearch kNN vector search
- `LLM_API_KEY`, `LLM_MODEL` for LiteLLM integration
- `ES_HOST` for Elasticsearch
- `LANGFUSE_*` for observability

Frontend uses `.env.local` with `FASTAPI_URL=http://localhost:8000`.

## Ports

| Service | Port |
|---------|------|
| Backend API | 8000 |
| Frontend (dev) | 3000 |
| LangFuse | 3001 |
| PostgreSQL | 5432 |
| Redis | 6379 |
| Elasticsearch | 9200 |
