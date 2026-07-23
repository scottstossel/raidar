# RAIDAR — Project context

## What it is
RAIDAR delivers up-to-date, high-quality AI research signal without the noise. It ingests material daily from sources like GitHub, arXiv, and HuggingFace, processes and enriches it, runs it through a multi-agent reasoning layer, and serves the output via an API.

## Architecture

**Pipeline (in order):**
1. **Sources** — GitHub, arXiv, HuggingFace (extensible to more later)
2. **Ingestion & cleaning** — fetch, dedupe, normalize (code, not LLM)
3. **Feature computation** — topic tagging, themes, trend scores, clustering, embeddings
4. **Intel layer** — multi-agent reasoning (see below)
5. **FastAPI service** — routes: `emerging`, `topics`, `brief`, `docs`

**Intel layer (multi-agent):**
- **Discovery agent** — finds new/relevant items, avoids duplicate work with ingestion
- **Analysis agent** — extracts signal per item (what it claims, why it matters)
- **Skeptic agent** — checks claims against sources, flags overhyped/unverified content; should run off a concrete checklist (source matches claim, benchmark comparisons are apples-to-apples, preprint vs peer-reviewed, real adoption signals vs just novelty) rather than an open-ended "be skeptical" instruction
- **Synthesis agent** — coordinates the above, drafts the final research brief
- **Outputs** — research brief (narrative) + knowledge & source index (citations, provenance)

**Cross-cutting concerns:**
- Evaluation across three separate surfaces (don't collapse into one metric):
  1. Feature/retrieval quality (trend scores, clustering)
  2. Agent output quality (skeptic catch rate on known bad claims, brief faithfulness to sources — LLM-as-judge with a golden set)
  3. End-to-end freshness/coverage (did today's brief surface what actually mattered)
- Monitoring/experiment tracking
- Alerts
- Orchestration + deployment infra

## Model routing decisions (as of this conversation)

Principle: **escalate, not blanket-use**. Cheap/fast models handle every item; Fable is reserved for a small number of high-stakes judgment calls, triggered by a signal (e.g. unusual trend velocity, high-impact source, an "uncertain" flag from a cheaper model) rather than used by default everywhere.

| Stage | Model | Reasoning |
|---|---|---|
| Ingestion & cleaning | No LLM | Deterministic parsing/dedup/normalization |
| Topic tagging / classification | Haiku 4.5 | High volume, fixed categories, needs speed/cost over depth |
| Embeddings | Dedicated embedding model | Not a chat-completion task |
| Trend scoring / clustering | No LLM | Statistical, runs on computed features |
| Discovery agent | Haiku 4.5 or Sonnet 5 | Mostly relevance filtering, repetitive |
| Analysis agent | Sonnet 5 | Per-item extraction at volume |
| Skeptic agent (default pass) | Sonnet 5 | Checklist-driven, runs on every item |
| Skeptic agent (escalated) | Fable 5 | Only for items already flagged high-impact |
| Synthesis / brief agent | Fable 5 | Low frequency, highest stakes — the actual deliverable |
| LLM-as-judge (eval) | Opus 4.8 or Fable 5, sampled | Should outrank whatever generated the output; sample daily, don't judge everything |
| Interactive query/chat route (if added) | Sonnet 5, escalate to Fable on demand | Latency matters more than max depth for most queries |

Fable is also the right tool **during development** (not in the running pipeline) for planning agent orchestration logic, debugging multi-agent failures, and designing the eval harness.

## Tooling notes from discussion
- **Orchestration**: prefer LangGraph or a lightweight custom state machine over heavier agent frameworks (CrewAI, AutoGen) since the flow is mostly linear with one fan-in point (synthesis), not open-ended agent-to-agent chatter.
- **Monitoring**: mlflow is suited to classical ML metrics (feature quality, clustering, embedding drift) but not built for LLM-specific tracing (prompts, agent traces, per-step latency/cost, judge scores). Decide early whether to pair it with an LLM-native tool (Langfuse, Phoenix, W&B Weave) rather than retrofitting tracing later.
- **Scheduling/infra**: a DAG-oriented orchestrator (Dagster, Prefect, or Airflow) above ingestion/feature/intel stages, with FastAPI as the read path, makes retries and alerting straightforward — alerts can hook off orchestrator failure/success events rather than being bespoke per stage.

## Open questions / not yet decided
- Specific vector store / embedding model choice
- Exact schema for the knowledge & source index
- Brief format and cadence (daily? per-topic?)
- Golden set design for the skeptic/judge evals
- Deployment target (cloud provider, containerization approach)

## Next steps under consideration
- Refine specific architecture pieces further, or
- Start scaffolding the actual repo structure and code