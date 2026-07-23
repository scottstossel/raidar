# RAIDAR — AI Research Signal Without the Noise

High-quality AI research briefs delivered daily, powered by a multi-agent reasoning pipeline.

## What It Does

RAIDAR ingests AI research from multiple sources (arXiv, GitHub, Hugging Face), enriches it with features (topic tagging, embeddings, trend scores), runs it through four specialized agents (discovery, analysis, skeptic, synthesis), and serves polished research briefs via a REST API.

## Architecture

```
Sources (arXiv, GitHub, HuggingFace)
    ↓
Ingestion & Dedup (3 adapters, hash-based dedup)
    ↓
Feature Computation (topics, embeddings, trends, themes)
    ↓
Intel Layer (4-agent pipeline)
    ├─ Discovery Agent: relevance filtering
    ├─ Analysis Agent: signal extraction
    ├─ Skeptic Agent: claim verification (Sonnet + Fable escalation)
    └─ Synthesis Agent: brief generation
    ↓
Persistence (PostgreSQL, Pinecone)
    ↓
FastAPI (5 REST endpoints)
```

## Phases Completed

### Phase 1: Ingestion Foundation
- PostgreSQL database schema (documents, dedup hashes, briefs, audit logs)
- Pydantic models for normalized documents
- arXiv adapter with rate limiting (reference implementation)
- Hash-based deduplication
- 5 unit tests ✅

### Phase 2: Remaining Sources
- GitHub adapter (repos + discussions)
- Hugging Face adapter (models + datasets)
- Multi-source ingestion orchestrator
- Airflow DAG skeleton
- 7 unit tests ✅

### Phase 3: Feature Computation
- Topic tagging (Haiku)
- Embeddings (Cohere + Pinecone)
- Trend scoring (statistical)
- Theme assignment (heuristic)
- 15 unit tests ✅

### Phase 4: Intel Layer
- Discovery agent (relevance filtering)
- Analysis agent (signal extraction)
- Skeptic agent (claim verification + Fable escalation)
- Synthesis agent (brief generation)
- Agent orchestration with escalation logic
- 23 unit tests ✅

### Phase 5: API & Persistence
- FastAPI application
- 5 REST endpoints (health, emerging, briefs, topics, docs)
- Brief persistence
- 27 unit tests ✅

## Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL 14+
- Pinecone account (for embeddings)
- Anthropic API key (Claude)
- Cohere API key (embeddings)

### Setup

1. **Clone and install**
   ```bash
   git clone <repo>
   cd raidar
   python -m venv venv
   source venv/bin/activate
   pip install -e .
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and DB connection
   ```

3. **Start local services**
   ```bash
   docker-compose up -d postgres redis
   ```

4. **Initialize database**
   ```bash
   python -c "from src.db.session import init_db; init_db()"
   ```

5. **Run tests**
   ```bash
   pytest tests/unit -v
   ```

6. **Start API**
   ```bash
   python -m uvicorn src.api.app:app --reload
   ```

### Run Pipeline Manually

```bash
# Ingest from all sources
python src/ingestion/run.py

# Compute features
python src/features/run.py

# Run intel pipeline (discovery → analysis → skeptic → synthesis)
python src/intel/run.py

# Briefs are now available at:
# GET /api/emerging         (today's brief)
# GET /api/briefs/topics/LLMs  (topic brief)
```

## API Endpoints

| Method | Path | Response |
|--------|------|----------|
| GET | `/api/health` | System status |
| GET | `/api/emerging` | Today's research brief |
| GET | `/api/briefs/topics/{topic}` | Topic-specific brief |
| GET | `/api/topics` | Available topics + counts |
| GET | `/api/docs/{id}` | Document detail + analysis |

Example:
```bash
curl http://localhost:8000/api/topics
```

## Model Routing

Per CLAUDE.md — escalate, don't blanket-use:

| Stage | Model | Rationale |
|-------|-------|-----------|
| Topic tagging | Haiku 4.5 | High volume, fixed categories |
| Embeddings | Cohere API | Dedicated embedding model |
| Trend scoring | Statistical | No LLM needed |
| Discovery | Haiku/Sonnet 5 | Mostly filtering |
| Analysis | Sonnet 5 | Per-item extraction |
| Skeptic (default) | Sonnet 5 | Checklist-driven |
| Skeptic (escalated) | Fable 5 | Only for flagged items |
| Synthesis | Fable 5 | Final deliverable |

## Observability

- **Langfuse** — LLM call tracing (cost, latency, decisions)
- **mlflow** — feature quality metrics (accuracy, drift)
- **Logs** — application events (ingestion, dedup, agent decisions)

## Testing

All 27 unit tests passing:
```
pytest tests/unit -v
```

Integration tests (require network):
```
pytest tests/integration -v -m integration
```

## Next Steps (Phase 6)

- [ ] Evaluation harness (golden sets, LLM judges)
- [ ] Daily evaluation runs with sampling
- [ ] Cost and latency dashboards
- [ ] Deployment (Docker, Kubernetes)
- [ ] On-demand per-topic brief API (Sonnet-tier latency)

## Development

**Adding a new source:**
1. Create `src/ingestion/<source>.py` adapter
2. Inherit fetch pattern from arXiv adapter
3. Test with integration tests
4. Wire into `src/ingestion/run.py`

**Modifying agents:**
1. Edit prompt in `src/intel/prompts/<agent>.txt`
2. Update parsing logic if response format changes
3. Test with unit tests
4. Langfuse will capture cost/latency automatically

**Tuning features:**
1. Add new feature in `src/features/`
2. Update `DocumentFeaturesORM` table
3. Wire into `src/features/run.py`
4. Add unit tests

## License

[Add license]

## Contact

scott@example.com

---

**Built with**: FastAPI, SQLAlchemy, Claude API, Anthropic SDK, Pinecone, Cohere API
