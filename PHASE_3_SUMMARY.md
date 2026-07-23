# Phase 3: Feature Computation — Complete

## What was built

### Feature computation modules
- **Topic tagging** (`src/features/topic_tagging.py`) — Haiku-based classification into fixed taxonomy; maps to primary + secondary topics with confidence scores
- **Embeddings** (`src/features/embeddings.py`) — Cohere API integration for embedding computation; batched upload to Pinecone with metadata
- **Trend scoring** (`src/features/trend_scoring.py`) — statistical scoring based on recency, velocity, and source credibility; no LLM
- **Theme assignment** (`src/features/themes.py`) — heuristic theme mapping (topic → theme + keyword fallback); lightweight, no ML

### Vector database
- **Pinecone client** (`src/vector/pinecone_client.py`) — wrapper for upsert, query, delete operations; handles Pinecone API calls

### Feature orchestration
- **Feature runner** (`src/features/run.py`) — orchestrates all feature stages, persists to `document_features` table, logs metrics

### Database
- **DocumentFeaturesORM** — new table for storing computed features (topic, theme, trend_score, embedding_id, etc.)

### Testing
- **Topic tagging tests** — response parsing, malformed responses, confidence clamping
- **Theme assignment tests** — topic mapping, keyword fallback, defaults
- **Trend scoring tests** — source weights, result structure
- All 15 unit tests passing

## Architecture decisions

1. **Haiku for topic tagging** — per CLAUDE.md model routing; high volume, fixed categories
2. **Cohere embeddings** — configured in config; easy to swap later
3. **No LLM for trend scoring** — statistical only (recency + velocity + source weight)
4. **Heuristic themes** — lightweight, no ML; maps topics + uses keyword detection
5. **Batch embeddings** — supports single and batch upsert to Pinecone

## What's ready for Phase 4

- Run feature computation on ingested documents
- Test end-to-end (ingestion → features)
- Intel layer agents can now query Pinecone for similar documents

## Testing status

All 15 unit tests passing:
```
$ pytest tests/unit -v
15 passed in 0.16s
```

## Files created/modified

**New:**
- `src/features/models.py` — Pydantic schemas for features
- `src/features/topic_tagging.py` — Haiku-based topic classification
- `src/features/embeddings.py` — Embedding service with Cohere + Pinecone
- `src/features/trend_scoring.py` — Statistical trend scoring
- `src/features/themes.py` — Heuristic theme assignment
- `src/features/run.py` — Feature orchestration
- `src/vector/pinecone_client.py` — Pinecone wrapper
- Tests for all feature modules

**Modified:**
- `src/db/models.py` — added DocumentFeaturesORM table
- `airflow/dags/daily_pipeline.py` — wired feature computation task

## Next: Phase 4 — Intel Layer

- Discovery agent (Haiku/Sonnet) — relevance filtering
- Analysis agent (Sonnet) — signal extraction
- Skeptic agent (Sonnet + Fable escalation) — claim verification
- Synthesis agent (Fable) — brief generation
