# Phase 5: API & Persistence — Complete

## What was built

### FastAPI application

- **App** (`src/api/app.py`) — FastAPI application with CORS middleware, startup/shutdown events
- **Routes** (`src/api/routes.py`) — 5 REST endpoints:
  - `GET /health` — system health check
  - `GET /emerging` — today's daily AI research brief
  - `GET /briefs/topics/{topic}` — topic-specific brief
  - `GET /topics` — list available topics with document counts
  - `GET /docs/{doc_id}` — full document details with analysis
- **Schemas** (`src/api/schemas.py`) — Pydantic models for all request/response types

### Persistence

- Brief persistence to database (BriefORM) with themes and metadata
- `persist_brief()` function in intel runner saves generated briefs
- Briefs queryable by type (daily or topic-specific)

### Testing

- 4 new unit tests for API schemas
- 27 total unit tests passing
- All schema models tested

## Routes

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | API root |
| GET | `/api/health` | System health |
| GET | `/api/emerging` | Daily brief |
| GET | `/api/briefs/topics/{topic}` | Topic brief |
| GET | `/api/topics` | Topic list |
| GET | `/api/docs/{doc_id}` | Document detail |

## Architecture decisions

1. **Stateless API** — routes query database directly; no caching layer (can add Redis later)
2. **Error handling** — HTTP exceptions with meaningful messages
3. **CORS open** — configured for all origins (restrict in production)
4. **Pydantic validation** — all responses validated against schemas

## End-to-end flow

```
Sources (arXiv, GitHub, HuggingFace)
  ↓ Ingestion → Dedup → Persistence
  ↓ Features (topics, embeddings, trends, themes)
  ↓ Intel (discovery, analysis, skeptic, synthesis)
  ↓ Persistence (briefs)
  ↓ API (serve briefs and documents)
  ↓ User (reads /emerging or /briefs/topics/...)
```

## What's ready for Phase 6

- Evaluation harness (golden sets + LLM judges)
- Sampling and daily evaluation runs
- Cost and latency dashboards (Langfuse + mlflow)
- Deployment (Docker, Kubernetes config)

## Testing status

All 27 unit tests passing:
```
$ pytest tests/unit -v
27 passed in 0.18s
```

## Files created/modified

**New:**
- `src/api/app.py` — FastAPI application
- `src/api/routes.py` — REST endpoints
- `src/api/schemas.py` — Pydantic models
- `tests/unit/test_api_schemas.py` — schema tests

**Modified:**
- `src/intel/run.py` — added brief persistence

## Next: Phase 6 — Evaluation & Monitoring

- Golden sets for skeptic and judge evals
- Evaluation harness (precision, recall, faithfulness)
- Daily sampling and running evals
- Cost and latency tracking (Langfuse, mlflow)
- Deployment infrastructure
