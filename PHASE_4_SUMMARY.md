# Phase 4: Intel Layer — Multi-Agent Reasoning — Complete

## What was built

### Four specialized agents

1. **Discovery Agent** (Haiku/Sonnet)
   - Filters documents for relevance using explicit criteria (novelty, relevance, signal, credibility)
   - Returns: relevance decision, confidence, reasoning, priority (if relevant)

2. **Analysis Agent** (Sonnet)
   - Extracts signal from each document: core claim, significance, limitations, applicability, key metrics
   - Returns JSON with structured signal

3. **Skeptic Agent** (Sonnet + Fable escalation)
   - Verifies claims against concrete checklist (source match, benchmark fairness, publication status, adoption signals, reproducibility)
   - Flags issues rather than free-form skepticism
   - Escalates to Fable when 2+ issues detected for deeper verification

4. **Synthesis Agent** (Fable)
   - Generates final research brief from curated documents
   - Weaves together related findings into coherent themes
   - Includes highlights, deep dives, caveats, and next steps
   - Low frequency, highest stakes—the actual deliverable

### Orchestration & persistence

- **IntelOrchestrator** — coordinates all four agents in sequence; manages escalation to Fable
- **Intel runner** — fetches documents with features, runs pipeline, supports daily and per-topic briefs

### Prompts (versioned separately)

- `discovery.txt` — explicit relevance criteria
- `analysis.txt` — structured signal extraction (JSON output)
- `skeptic_checklist.txt` — concrete verification checklist (not free-form skepticism)
- `synthesis.txt` — brief generation framework

### Testing

- 8 new unit tests for agents (discovery, analysis, skeptic, synthesis)
- Tests verify parsing, error handling, escalation logic
- All 23 unit tests passing

## Architecture decisions

1. **Model routing per agent** — Discovery: Haiku/Sonnet, Analysis: Sonnet, Skeptic: Sonnet + Fable escalation, Synthesis: Fable
2. **Skeptic escalation logic** — starts with Sonnet; escalates to Fable if 2+ issues found (cost optimization)
3. **Structured output** — JSON for analysis, checklist format for skeptic (not free-form text)
4. **Langfuse instrumentation** — LLM calls and agent decisions logged for observability and cost tracking
5. **Linear pipeline** — discovery → analysis → skeptic → synthesis (no agent-to-agent chatter)

## Pipeline flow

```
Document (with features)
  ↓
Discovery Agent: filter by relevance
  ↓ (passes)
Analysis Agent: extract signal
  ↓
Skeptic Agent (Sonnet): verify claims
  ↓ (if 2+ flags)
Skeptic Agent (Fable): escalated verification
  ↓
Synthesis Agent: generate brief
  ↓
Brief (daily or per-topic)
```

## What's ready for Phase 5

- Database persistence for briefs
- API layer to serve briefs
- Evaluation harness (golden sets + judges)
- End-to-end testing (ingestion → features → intel → API)

## Testing status

All 23 unit tests passing:
```
$ pytest tests/unit -v
23 passed in 0.19s
```

## Files created/modified

**New:**
- `src/intel/agents/discovery.py` — relevance filtering agent
- `src/intel/agents/analysis.py` — signal extraction agent
- `src/intel/agents/skeptic.py` — claim verification agent
- `src/intel/agents/synthesis.py` — brief generation agent
- `src/intel/orchestration.py` — multi-agent coordinator
- `src/intel/run.py` — intel pipeline runner
- `src/intel/prompts/` — versioned prompt templates
- Tests for all four agents

**Modified:**
- `airflow/dags/daily_pipeline.py` — wired intel task

## Next: Phase 5 — API & Persistence

- Save briefs to database
- FastAPI routes: emerging, topics, brief, docs
- Request/response schemas
- API tests
