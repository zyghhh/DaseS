# DaseS - CS Academic Paper Multi-Agent Retrieval System

## Project Overview

DaseS is a sophisticated multi-agent retrieval system specifically designed for computer science academic papers. It leverages a hybrid search approach (Vector + BM25) and coordinates specialized AI agents to provide deep analysis, summarization, and retrieval of scholarly literature.

### Architecture
- **Backend (Python + FastAPI):** Orchestrates multi-agent workflows using **LangGraph**. It features a RAG pipeline with **Milvus** for vector search and **Elasticsearch** for BM25 keyword search.
- **Frontend (Next.js 15 + TypeScript):** A modern interactive interface using the App Router, **TanStack Query** for state management, and **React Flow** for agent trace visualization.
- **Multi-Agent System:**
  - **Coordinator:** Intent recognition and task dispatching.
  - **Retriever:** Hybrid paper retrieval (Vector + BM25 + RRF).
  - **Summarizer:** Single/multi-paper abstract generation.
  - **Analyzer:** Methodology comparison and trend analysis.
- **Infrastructure:** Containerized services via Docker Compose (PostgreSQL, Redis, Milvus, Elasticsearch, LangFuse).

### Key Technologies
- **LLM Orchestration:** LangGraph, LangChain, LiteLLM.
- **Storage:** Milvus (Vector), Elasticsearch (BM25), PostgreSQL (Relational), Redis (Cache).
- **Observability:** LangFuse for agent tracing.
- **UI/UX:** Tailwind CSS, Shadcn UI, Lucide React.

---

## Building and Running

### Prerequisites
- **uv:** Fast Python package manager.
- **Docker & Docker Compose:** For infrastructure services.
- **Node.js:** For frontend development.

### 1. Infrastructure Setup
Start the required databases and services:
```bash
docker compose up -d
```

### 2. Backend (FastAPI)
```bash
cd backend
# Sync dependencies and create venv
uv sync --extra dev

# Setup environment
cp .env.example .env  # Update with your LLM API keys and DB URLs

# Run development server
uv run uvicorn app.main:app --reload
```

### 3. Frontend (Next.js)
```bash
cd frontend
# Install dependencies
npm install

# Run development server
npm run dev
```

### 4. Data Ingestion
Scripts for processing OpenAlex and DBLP data are located in `data/scripts/`.
```bash
# Example for S2AG abstract ingestion (see data/scripts/README.md for details)
uv run data/scripts/ingest_s2ag_abstracts.py --es-host http://localhost:9200
```

---

## Development Conventions

### Backend
- **Async First:** Use `async/await` for all DB and API calls (SQLAlchemy async, Elasticsearch async client).
- **Type Safety:** All data models use **Pydantic v2** (`app/models/schemas.py`).
- **Configuration:** Managed via `pydantic-settings` in `app/core/config.py`.
- **Linting & Formatting:** 
  - `uv run ruff check .`
  - `uv run ruff format .`
- **Testing:** 
  - `uv run pytest`

### Frontend
- **BFF Pattern:** Client calls go through Next.js API Routes (`src/app/api/`) to proxy to the FastAPI backend.
- **Server Components:** Prefer Server Components; use `'use client'` only for interactive logic.
- **State Management:** **TanStack Query v5** for server state; custom hooks for SSE streaming (`use-chat.ts`).
- **Styling:** Tailwind CSS with Shadcn UI components.
- **Visualizations:** Agent traces are rendered using **React Flow** (@xyflow/react).
- **Testing:** 
  - `npm run test` (Vitest for units)
  - `npm run test:e2e` (Playwright for E2E)

### Agent Tracing
All agent runs are logged to **LangFuse** for observability. Ensure `LANGFUSE_SECRET_KEY` and `LANGFUSE_PUBLIC_KEY` are configured in the backend `.env`.
