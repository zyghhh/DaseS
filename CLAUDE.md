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
uv run uvicorn app.main:app --reload

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
# Start all services (PostgreSQL, Redis, Milvus, Elasticsearch, LangFuse)
docker compose up -d

# Start specific services
docker compose up -d postgres redis milvus elasticsearch langfuse
```

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
- `VECTOR_DB_HOST/PORT` for Milvus
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
| Milvus | 19530 |
| Elasticsearch | 9200 |
