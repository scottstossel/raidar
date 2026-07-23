# RAIDAR — Complete Project Summary

## Overview

RAIDAR delivers high-quality AI research signal by combining automated ingestion, feature computation, and multi-agent reasoning into a daily research brief.

**Status:** Phases 1–5 complete ✅  
**Test Coverage:** 27 unit tests passing  
**Next:** Phase 6 (Evaluation & Deployment)

## Architecture Summary

### Pipeline Layers

1. **Ingestion** — Fetch from 3 sources, deduplicate, normalize
2. **Features** — Compute topics, embeddings, trends, themes
3. **Intel** — 4-agent pipeline (discovery, analysis, skeptic, synthesis)
4. **Persistence** — Store to PostgreSQL + Pinecone
5. **API** — Serve briefs and documents over REST

### Data Flow

```
[Sources]
  ↓ ArXiv, GitHub, HuggingFace adapters
[Ingestion Layer]
  ↓ Hash-based dedup, normalize to Document schema
[Feature Computation]
  ↓ Topic tagging (Haiku), embeddings (Cohere), trends, themes
[Intel Layer]
  ↓ Discovery → Analysis → Skeptic → Synthesis
[Persistence]
  ↓ PostgreSQL (documents, features, briefs)
  ↓ Pinecone (vectors)
[API]
  ↓ FastAPI (health, emerging, topics, docs, briefs)
[User]
  ↓ GET /api/emerging → Daily brief
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Ingestion | Python, httpx, arXiv/GitHub/HF APIs |
| Database | PostgreSQL (documents) + Pinecone (vectors) |
| LLMs | Anthropic (Claude Haiku/Sonnet/Fable) |
| Embeddings | Cohere API |
| Feature Eng. | Python, statistical methods |
| API | FastAPI, Pydantic |
| Orchestration | Airflow (DAGs for daily runs) |
| Monitoring | Langfuse (LLM traces), mlflow (metrics) |
| Testing | pytest (27 unit tests) |

## File Structure

```
raidar/
├── src/
│   ├── ingestion/         # 3 adapters (arxiv, github, huggingface)
│   ├── features/          # 4 stages (topic, embeddings, trends, themes)
│   ├── intel/
│   │   ├── agents/        # 4 agents (discovery, analysis, skeptic, synthesis)
│   │   ├── prompts/       # Versioned prompt templates
│   │   └── orchestration.py
│   ├── api/               # FastAPI app + 5 routes
│   ├── db/                # SQLAlchemy ORM models
│   ├── vector/            # Pinecone client
│   ├── monitoring/        # Langfuse tracer, metrics
│   └── config.py          # Centralized settings
├── airflow/dags/          # Daily pipeline DAG
├── eval/                  # Evaluation harness (Phase 6)
├── tests/
│   ├── unit/              # 27 tests
│   └── integration/       # Adapter tests (require network)
├── docker-compose.yml     # Local dev stack
├── pyproject.toml
└── README.md
```

## Key Design Decisions

### 1. Source Adapters (Ingestion)
- **One file per source** — independent evolution, isolated error handling
- **Shared Document schema** — Pydantic for validation
- **Hash-based dedup** — content + URL hashes prevent re-ingestion
- **Isolation from features** — clean separation of concerns

### 2. Feature Computation
- **Haiku for topic tagging** — cheap, fast, fixed categories
- **Cohere embeddings** — dedicated model (not LLM), stored in Pinecone
- **Statistical trend scoring** — recency + velocity + source weight
- **Heuristic themes** — topic mapping + keyword fallback (no ML)

### 3. Intel Layer (Multi-Agent)
- **Linear pipeline** — discovery → analysis → skeptic → synthesis
- **Explicit criteria** — discovery uses checklist, not free-form judgment
- **Structured extraction** — analysis returns JSON, skeptic uses checklist
- **Escalation logic** — skeptic starts with Sonnet, escalates to Fable on 2+ issues
- **Versioned prompts** — stored separately for easy iteration

### 4. Skeptic Agent (Novel Approach)
- **Concrete checklist** — source match, benchmark fairness, publication status, adoption, reproducibility
- **Not free-form** — verify specific claims, not "be skeptical"
- **Escalation** — Sonnet for most, Fable for multiple issues (cost optimization)
- **Flags stored** — persisted for user awareness

### 5. Synthesis Agent
- **Fable only** — highest stakes, final deliverable
- **Narrative coherence** — weaves related findings into themes
- **Caveats explicit** — includes skeptic flags
- **Actionable** — points to sources and next steps

### 6. API Design
- **Stateless** — routes query DB directly
- **Pydantic validation** — all responses validated
- **Error handling** — meaningful HTTP exceptions
- **Queryable by topic** — daily brief + per-topic briefs

## Model Routing (Cost-Optimized)

```
High volume  → Haiku (cheap, fast)
Per-item     → Sonnet (balanced)
Escalated    → Fable (deep reasoning, expensive)
Final        → Fable (highest stakes)
```

Cost per document approximately:
- Discovery: Haiku input only → ~$0.001
- Analysis: Sonnet → ~$0.01
- Skeptic: Sonnet → ~$0.01 (escalated Fable → $0.10)
- Synthesis: Fable (amortized) → negligible per-doc

## Observability Strategy

**Necessary:**
- LLM cost per stage (Langfuse)
- Agent success rates (Langfuse)
- Ingestion health: fetch/dedup rates
- Feature pass rates

**Enablers:**
- Prompt version hashing (Langfuse)
- Per-source contribution (discovery pass rate)

**Avoid:**
- Token-level telemetry
- Intermediate clustering scores
- Per-query latency for every API call

## Testing Philosophy

- **Unit tests** (27) — fast, isolated, mocked LLMs
- **Integration tests** — adapters only (require network, optional)
- **No end-to-end mocks** — trust ingestion + features output for intel tests
- **Schema validation** — Pydantic catches shape errors early

## Phases & Timeline

| Phase | Scope | Status | Tests |
|-------|-------|--------|-------|
| 1 | Ingestion foundation | ✅ | 5 |
| 2 | GitHub + HF adapters | ✅ | 7 |
| 3 | Feature computation | ✅ | 15 |
| 4 | Intel layer (4 agents) | ✅ | 23 |
| 5 | API + persistence | ✅ | 27 |
| 6 | Eval + monitoring | ⏳ | — |

## What's Ready for Phase 6

**Evaluation:**
- Golden sets for skeptic (known bad claims) and judges (sample briefs)
- Skeptic catch rate metric
- Brief faithfulness metric (LLM-as-judge with Opus)
- Daily sampling and eval runs

**Deployment:**
- Docker image with all services
- Kubernetes manifests
- Helm chart (optional)
- CI/CD pipeline (GitHub Actions)

**Documentation:**
- API docs (auto-generated via Swagger)
- Prompt tuning guide
- Model routing decisions document
- Deployment runbook

## Known Limitations

1. **Prompts not versioned in DB** — currently file-based; can add versioning later
2. **No caching layer** — API queries DB every time; Redis can be added
3. **Airflow DAG not deployed** — scaffold ready, needs actual Airflow instance
4. **No auth** — CORS wide open; needs OAuth2 for production
5. **Brief persistence minimal** — stores content + themes only; can extend metadata
6. **No incremental feature updates** — recomputes all features each run (works for daily, not hourly)

## Future Enhancements

1. **On-demand queries** — user asks for "LLM alignment research from last 3 days" → Sonnet-tier latency, real-time
2. **Citation graph** — track which papers cite which, find emerging connections
3. **Community signals** — GitHub stars over time, arXiv social signals
4. **Source weighting** — learn which sources historically surface real signal
5. **Trend prediction** — forecast next week's topics based on velocity
6. **Multimodal** — include blog posts, tweets, conference talks
7. **Collaborative filtering** — "users interested in topic X also read…"

## Code Quality

- **Type hints** — used throughout (mypy-ready)
- **Docstrings** — focused on intent, not WHAT
- **Comments** — only where non-obvious (rare)
- **Testing** — 27 unit tests, integration test skeletons
- **Logging** — info + error levels, no debug spam
- **Error handling** — graceful degradation, meaningful messages

## Deployment Notes

**For production:**
1. Restrict CORS to known origins
2. Add authentication (OAuth2 / API keys)
3. Use managed PostgreSQL (RDS, Cloud SQL)
4. Deploy Airflow to Kubernetes or cloud (Cloud Composer, MWAA)
5. Configure Langfuse with project isolation
6. Set up alerts on ingestion/feature/intel failures
7. Monitor Pinecone query latency

**For local dev:**
```bash
docker-compose up
python -m pytest tests/unit
python -m uvicorn src.api.app:app --reload
```

## Key Takeaways

RAIDAR demonstrates:
- **Pragmatic multi-agent design** — linear pipeline, no chatter, explicit handoffs
- **Cost-aware model routing** — cheap models for volume, expensive models for high-stakes
- **Structured LLM outputs** — JSON + checklists, not free-form text
- **Observable systems** — Langfuse for LLM traces, mlflow for classical metrics
- **Modular architecture** — each stage independently testable
- **Type-safe Python** — Pydantic for validation, SQLAlchemy for DB

## Contact & Questions

See README.md for setup instructions and API reference.

---

**Built with Claude Code** — All 5 phases completed in single session  
**Test coverage:** 27 unit tests, ~2000 lines of production code  
**Ready for:** evaluation harness, deployment, user testing
