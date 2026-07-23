# RAIDAR — Project context for Claude Code

## What this project is
RAIDAR delivers up-to-date, high-quality AI research signal without the noise. It ingests material daily from sources like GitHub, arXiv, and HuggingFace, processes and enriches it, runs it through a multi-agent reasoning layer, and serves the output via an API.

## Architecture (target state)

Pipeline, in order:
1. **Sources** — GitHub, arXiv, HuggingFace (design for adding more later)
2. **Ingestion & cleaning** — fetch, dedupe, normalize. Plain code, no LLM calls.
3. **Feature computation** — topic tagging, themes, trend scores, clustering, embeddings
4. **Intel layer** — multi-agent reasoning (see below)
5. **API** — FastAPI, routes: `emerging`, `topics`, `brief`, `docs`

### Intel layer agents
- **Discovery agent** — finds new/relevant items; must not duplicate ingestion's dedup work
- **Analysis agent** — extracts signal per item (what it claims, why it matters)
- **Skeptic agent** — verifies claims against sources using a concrete checklist (does the claim match the source, is the benchmark comparison fair, preprint vs peer-reviewed, real adoption vs just novelty). Not a free-form "be skeptical" prompt — implement it as explicit checks.
- **Synthesis agent** — coordinates the above, drafts the final brief
- Outputs: a research brief (narrative) and a knowledge & source index (citations, provenance)

## Model routing — follow this, don't default to one model everywhere

Escalate to stronger/costlier models only on signal, not by default:

| Stage | Model |
|---|---|
| Ingestion & cleaning | No LLM |
| Topic tagging / classification | Haiku 4.5 |
| Embeddings | Dedicated embedding model, not a chat model |
| Trend scoring / clustering | No LLM (statistical) |
| Discovery agent | Haiku 4.5 or Sonnet 5 |
| Analysis agent | Sonnet 5 |
| Skeptic agent (default pass, every item) | Sonnet 5 |
| Skeptic agent (escalated, flagged items only) | Fable 5 |
| Synthesis / brief agent | Fable 5 |
| Eval judge | Opus 4.8 or Fable 5, sampled — never judge with the same tier that generated the output |

When implementing a new agent or pipeline stage, default to the model in this table rather than picking the most powerful one available.

## Tooling decisions already made
- **Orchestration**: LangGraph or a lightweight custom state machine — avoid CrewAI/AutoGen, the flow is linear with one fan-in point (synthesis), not open agent-to-agent chatter.
- **Monitoring**: mlflow for classical ML metrics (feature quality, clustering, embedding drift) is fine, but it is not built for LLM tracing. Pair it with an LLM-native tracer (Langfuse, Phoenix, or W&B Weave) for agent traces, prompts, and per-step cost/latency rather than trying to force everything into mlflow.
- **Scheduling**: a DAG orchestrator (Dagster, Prefect, or Airflow) above ingestion/feature/intel stages; FastAPI is the read path only. Alerts should hook off orchestrator failure/success events, not be bespoke per stage.

## Evaluation — keep these three surfaces separate, don't collapse into one score
1. Feature/retrieval quality (trend scores, clustering)
2. Agent output quality (skeptic catch rate on a golden set of known bad claims; brief faithfulness to sources via LLM-as-judge)
3. End-to-end freshness/coverage (did today's brief surface what actually mattered)

## Open decisions (ask before assuming)
- Vector store / embedding model choice
- Exact schema for the knowledge & source index
- Brief cadence (daily vs per-topic) and format
- Golden set design for skeptic/judge evals
- Deployment target and containerization approach

## Working conventions
- Prefer explaining a plan before writing code for any multi-file or cross-stage change — this is a multi-stage system where changes in one stage can break assumptions in another.
- When a design decision isn't listed above as settled, ask rather than assume, and update this file once decided.
- Every new pipeline stage should be independently evaluable — don't add a stage without a way to measure its output quality.