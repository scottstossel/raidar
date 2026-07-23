# Phase 6: Evaluation & Deployment Guide

## Overview

Phase 6 focuses on measuring system quality (evaluation), observing production behavior (monitoring), and deploying to production (infrastructure). This phase validates that the multi-agent pipeline surfaces real signal.

**Estimated effort:** 2–3 weeks  
**Dependencies:** Phases 1–5 complete ✅

---

## Part 1: Evaluation Harness

### Goal
Measure pipeline quality across three independent surfaces:
1. **Feature quality** — do computed topics/trends match reality?
2. **Agent output quality** — do agents surface real signal?
3. **End-to-end quality** — do briefs actually matter to users?

### 1.1 Build Golden Sets

Golden sets are curated datasets with ground truth for evaluation.

**Directory structure:**
```
eval/golden_sets/
├── skeptic_claims.json          # 20–50 known-bad claims
├── brief_examples.json          # 5–10 example briefs with ratings
└── README.md                    # How to extend
```

**skeptic_claims.json** — Known-bad claims for skeptic catch rate:
```json
[
  {
    "title": "Revolutionary LLM 100x Faster",
    "source": "arxiv",
    "core_claim": "New architecture achieves 100x speedup with no quality loss",
    "content": "We propose a new attention mechanism...",
    "expected_flags": ["benchmark_unfair", "adoption_unverified"],
    "severity": "high",
    "reason": "Claims extreme improvement without proper baselines"
  },
  {
    "title": "Graph Neural Networks Outperform Transformers on Vision",
    "source": "arxiv",
    "core_claim": "GNNs are superior to transformers for image tasks",
    "content": "We evaluate GNNs on CIFAR-10...",
    "expected_flags": ["benchmark_unfair"],
    "severity": "medium",
    "reason": "Unfair comparison (different training regimes)"
  },
  {
    "title": "Ethical AI Framework Deployed at Scale",
    "source": "github_discussion",
    "core_claim": "Framework now used by 10,000+ companies",
    "content": "Our ethical AI framework is production-ready...",
    "expected_flags": ["adoption_unverified"],
    "severity": "medium",
    "reason": "No evidence of actual adoption"
  }
]
```

**brief_examples.json** — Example briefs with human ratings:
```json
[
  {
    "date": "2024-01-15",
    "brief_type": "daily",
    "content": "# Daily AI Research Brief\n\n## 🔥 Highlights\n...",
    "human_rating": {
      "signal_quality": 4,  # 1-5 scale
      "novelty": 4,
      "actionability": 3,
      "accuracy": 5,
      "comments": "Excellent coverage of emerging LLM trends"
    },
    "included_documents": [2024, 2015, 2018],  # doc IDs
    "themes": ["LLMs", "Inference"]
  }
]
```

**How to build:**
1. Run pipeline for 3–5 days, collect output
2. Read briefs critically; note which documents/claims feel wrong
3. Create "bad claim" entries for skeptic to catch
4. Collect human ratings for briefs (have 2–3 people rate each)
5. Store in git; version alongside code

### 1.2 Implement Evaluation Metrics

Create `eval/metrics.py`:

```python
"""Evaluation metrics for RAIDAR."""

from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime

@dataclass
class SkepticEvalResult:
    """Skeptic agent evaluation results."""
    total_claims: int
    caught_flags: int  # Correctly identified issues
    missed_flags: int  # Should have flagged but didn't
    false_positives: int  # Flagged but shouldn't have
    catch_rate: float  # caught_flags / (caught_flags + missed_flags)
    precision: float  # caught_flags / (caught_flags + false_positives)
    timestamp: datetime

@dataclass
class BriefEvalResult:
    """Brief faithfulness evaluation (LLM-as-judge)."""
    brief_id: int
    faithfulness_score: float  # 0-1 (does brief accurately represent sources?)
    signal_quality_score: float  # 0-1 (is this actually important research?)
    coherence_score: float  # 0-1 (are themes well-integrated?)
    overall_score: float  # average of above
    judge_reasoning: str  # why judge gave these scores
    timestamp: datetime

@dataclass
class FeatureEvalResult:
    """Feature quality evaluation."""
    stage: str  # "topic_tagging", "trend_scoring", "themes"
    accuracy: float  # for classifiers
    drift: float  # embedding/feature drift detection
    coverage: float  # % of documents with valid features
    timestamp: datetime
```

Implement evaluation functions:

```python
def evaluate_skeptic(golden_set: List[Dict]) -> SkepticEvalResult:
    """Evaluate skeptic agent on golden set of known-bad claims."""
    from src.intel.agents.skeptic import SkepticAgent
    
    skeptic = SkepticAgent()
    caught = 0
    missed = 0
    
    for claim in golden_set:
        result = skeptic.verify_claims(
            document_id=-1,
            title=claim["title"],
            source=claim["source"],
            core_claim=claim["core_claim"],
            content=claim["content"]
        )
        
        expected_flags = set(claim["expected_flags"])
        actual_flags = set(result["flags"])
        
        # Did skeptic catch at least one expected flag?
        if actual_flags & expected_flags:
            caught += 1
        else:
            missed += 1
    
    return SkepticEvalResult(
        total_claims=len(golden_set),
        caught_flags=caught,
        missed_flags=missed,
        false_positives=0,  # TODO: implement
        catch_rate=caught / len(golden_set),
        precision=0.0,  # TODO: implement
        timestamp=datetime.utcnow()
    )

def evaluate_brief(brief_content: str, brief_id: int) -> BriefEvalResult:
    """Evaluate brief using LLM-as-judge (Opus)."""
    from anthropic import Anthropic
    from src.config import settings
    
    client = Anthropic(api_key=settings.anthropic_api_key)
    
    prompt = f"""Evaluate this research brief for quality and accuracy.

Brief:
{brief_content}

Score these dimensions (0-1 scale):
1. Faithfulness: Does the brief accurately represent its sources?
2. Signal quality: Are these actually important research findings?
3. Coherence: Are themes well-integrated and logical?

Respond in JSON:
{{
  "faithfulness_score": <0-1>,
  "signal_quality_score": <0-1>,
  "coherence_score": <0-1>,
  "reasoning": "<1-2 sentences>"
}}"""
    
    response = client.messages.create(
        model=settings.model_judge,
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}]
    )
    
    # Parse JSON, compute overall score
    # ...implementation
    
    return BriefEvalResult(
        brief_id=brief_id,
        faithfulness_score=0.85,
        signal_quality_score=0.82,
        coherence_score=0.88,
        overall_score=0.85,
        judge_reasoning="...",
        timestamp=datetime.utcnow()
    )
```

### 1.3 Run Daily Evaluation

Create `eval/runner.py`:

```python
"""Daily evaluation runner."""

import logging
from datetime import datetime
from sqlalchemy import text
from src.db.session import get_db_session
from eval.metrics import (
    evaluate_skeptic, evaluate_brief,
    SkepticEvalResult, BriefEvalResult
)

def run_daily_eval():
    """Run evaluation on yesterday's brief."""
    db = get_db_session()
    
    # 1. Load yesterday's daily brief
    query = text("""
        SELECT id, content FROM briefs
        WHERE brief_type = 'daily'
        AND DATE(generated_at) = CURRENT_DATE - 1
        LIMIT 1
    """)
    result = db.execute(query).fetchone()
    if not result:
        logging.warning("No brief found for yesterday; skipping eval")
        return
    
    brief_id, brief_content = result
    
    # 2. Evaluate brief faithfulness
    brief_eval = evaluate_brief(brief_content, brief_id)
    logging.info(f"Brief eval: faithfulness={brief_eval.faithfulness_score:.2f}")
    
    # Persist eval result
    insert_query = text("""
        INSERT INTO eval_results
        (brief_id, eval_type, faithfulness_score, signal_quality_score, 
         coherence_score, timestamp)
        VALUES (:brief_id, 'brief', :f, :s, :c, :ts)
    """)
    db.execute(insert_query, {
        "brief_id": brief_id,
        "f": brief_eval.faithfulness_score,
        "s": brief_eval.signal_quality_score,
        "c": brief_eval.coherence_score,
        "ts": datetime.utcnow()
    })
    db.commit()
    
    # 3. Sample documents and evaluate features
    # (topic accuracy, trend prediction accuracy, etc.)
    
    logging.info("Daily evaluation complete")

if __name__ == "__main__":
    run_daily_eval()
```

**Schedule:** Run at 8am daily via Airflow (after daily pipeline completes at midnight).

---

## Part 2: Monitoring & Cost Tracking

### 2.1 Langfuse Dashboard Setup

Langfuse already instrumented via `src/monitoring/langfuse_tracer.py`.

**To set up dashboard:**
1. Log into [langfuse.com](https://langfuse.com)
2. Create project "RAIDAR"
3. Copy public/secret keys to `.env`
4. Traces will automatically flow from agents

**Key metrics to track:**
- **Cost per brief** = sum(discovery + analysis + skeptic + synthesis costs)
- **Latency per stage** = p50, p99 latency for each agent
- **Error rate** = % of documents that fail any stage
- **Model usage** = token counts by model (Haiku vs Sonnet vs Fable)

**Sample Langfuse queries:**
```
# Cost per brief (daily)
SELECT 
  DATE(timestamp),
  SUM(usage.cost_usd) as total_cost
FROM traces
WHERE metadata.brief_type = 'daily'
GROUP BY DATE(timestamp)

# Skeptic escalation rate
SELECT 
  COUNT(*) FILTER (WHERE metadata.escalated = true) * 100.0 / COUNT(*) 
FROM traces
WHERE name = 'skeptic'
```

### 2.2 mlflow Metrics Setup

Feature quality and drift detection via mlflow.

**Create `src/monitoring/mlflow_setup.py`:**

```python
"""Initialize mlflow for feature quality tracking."""

import mlflow
from src.config import settings

mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
mlflow.set_experiment("raidar_features")

def log_feature_quality(
    stage: str,  # "topic_tagging", "embeddings", etc.
    accuracy: float = None,
    drift: float = None,
    coverage: float = None,
):
    """Log feature quality metrics."""
    with mlflow.start_run(run_name=f"{stage}_{datetime.utcnow().isoformat()}"):
        if accuracy is not None:
            mlflow.log_metric("accuracy", accuracy)
        if drift is not None:
            mlflow.log_metric("embedding_drift", drift)
        if coverage is not None:
            mlflow.log_metric("feature_coverage", coverage)
```

**Track:**
- Topic classification accuracy (sample 10 docs/day, human review)
- Embedding drift (cosine distance between consecutive days)
- Feature coverage (% of documents with all features)
- Trend score calibration (predicted trends vs actual engagement)

### 2.3 Alerts Setup

Add to Airflow DAG failure notifications:

```python
from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

slack_webhook = Variable.get("SLACK_WEBHOOK_URL")

def alert_on_failure(context):
    """Send Slack alert on pipeline failure."""
    task = context['task']
    msg = f"❌ {task.task_id} failed in {context['dag'].dag_id}"
    SlackWebhookOperator(
        task_id="slack_alert",
        http_conn_id="slack",
        message=msg,
        webhook_token=slack_webhook
    ).execute(context)

dag.default_args['on_failure_callback'] = alert_on_failure
```

---

## Part 3: Deployment

### 3.1 Containerization

Create `Dockerfile`:

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install -e .

# Copy source
COPY src src
COPY airflow airflow

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "from src.db.session import engine; engine.connect()"

ENTRYPOINT ["python"]
CMD ["-m", "uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Build:**
```bash
docker build -t raidar:latest .
docker tag raidar:latest raidar:v0.1.0
```

### 3.2 Docker Compose for Production-Like Local Testing

Update `docker-compose.yml`:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: raidar
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: raidar
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U raidar"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://raidar:${DB_PASSWORD}@postgres:5432/raidar
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
      COHERE_API_KEY: ${COHERE_API_KEY}
      PINECONE_API_KEY: ${PINECONE_API_KEY}
      LANGFUSE_PUBLIC_KEY: ${LANGFUSE_PUBLIC_KEY}
      LANGFUSE_SECRET_KEY: ${LANGFUSE_SECRET_KEY}
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/api/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  airflow-webserver:
    image: apache/airflow:2.7-python3.10
    environment:
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://raidar:${DB_PASSWORD}@postgres:5432/airflow
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
    command: webserver

volumes:
  postgres_data:
```

**Run:**
```bash
docker-compose up -d
# API at http://localhost:8000
# Airflow at http://localhost:8080
```

### 3.3 Kubernetes Deployment

Create `k8s/raidar-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: raidar-api
  namespace: default
spec:
  replicas: 2
  selector:
    matchLabels:
      app: raidar-api
  template:
    metadata:
      labels:
        app: raidar-api
    spec:
      containers:
      - name: api
        image: raidar:v0.1.0
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: raidar-secrets
              key: database-url
        - name: ANTHROPIC_API_KEY
          valueFrom:
            secretKeyRef:
              name: raidar-secrets
              key: anthropic-api-key
        # ... other env vars
        livenessProbe:
          httpGet:
            path: /api/health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /api/health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: raidar-api-service
spec:
  selector:
    app: raidar-api
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8000
  type: LoadBalancer
```

**Deploy:**
```bash
kubectl create secret generic raidar-secrets \
  --from-literal=database-url=... \
  --from-literal=anthropic-api-key=...

kubectl apply -f k8s/
kubectl get pods -l app=raidar-api
```

### 3.4 CI/CD Pipeline (GitHub Actions)

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy RAIDAR

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -e .
      - run: pytest tests/unit -v
      - run: pytest tests/integration -v -m integration || true  # Optional

  build:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: docker/setup-buildx-action@v2
      - uses: docker/login-action@v2
        with:
          registry: gcr.io
          username: _json_key
          password: ${{ secrets.GCP_SA_KEY }}
      - uses: docker/build-push-action@v4
        with:
          push: true
          tags: gcr.io/raidar-prod/api:${{ github.sha }}

  deploy:
    needs: build
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: azure/setup-kubectl@v3
      - run: |
          kubectl set image deployment/raidar-api \
            api=gcr.io/raidar-prod/api:${{ github.sha }} \
            --namespace=production
```

---

## Part 4: Deployment Checklist

### Pre-Production

- [ ] All 27 unit tests passing
- [ ] Integration tests passing for all adapters
- [ ] Evaluation harness complete (golden sets + metrics)
- [ ] 1 week of daily briefs generated and reviewed
- [ ] Skeptic catch rate > 80%
- [ ] Brief faithfulness score > 0.85 (LLM judge)
- [ ] Cost per brief < $0.50
- [ ] Latency per brief < 2 minutes
- [ ] Zero critical bugs in ingestion/features/intel

### Staging Deployment

- [ ] Docker image builds successfully
- [ ] Kubernetes manifests validated
- [ ] Environment variables configured
- [ ] Database migrations run without errors
- [ ] Langfuse tracing working
- [ ] mlflow metrics flowing
- [ ] Slack alerts configured
- [ ] Load test (100 concurrent requests to /api/emerging)

### Production Deployment

- [ ] Monitoring dashboards visible (Langfuse, mlflow)
- [ ] Alert thresholds tuned (don't alert for expected variance)
- [ ] Backup strategy for PostgreSQL and Pinecone
- [ ] Rollback procedure documented
- [ ] On-call runbook written
- [ ] SLOs documented (99.5% uptime, < 1s p99 latency)
- [ ] CORS configured for specific origins
- [ ] Rate limiting enabled (e.g., 100 req/min per IP)
- [ ] Security review done (no secrets in code, API keys in secrets manager)

### Day 1 Post-Launch

- [ ] Monitor Langfuse dashboard hourly
- [ ] Check that evaluation runs automatically
- [ ] Manually verify 3–5 briefs (spot check for quality)
- [ ] Confirm Slack alerts work
- [ ] Track first user feedback

---

## Part 5: Operations Runbook

### Daily Operations

1. **8am:** Daily brief published at `/api/emerging`
   - Check Langfuse for cost/latency spikes
   - Verify brief in Slack notification
   
2. **9am:** Evaluation runs
   - Check mlflow for feature drift
   - Review LLM judge score on brief
   
3. **Throughout day:** Monitor logs
   - Ingestion errors (e.g., arXiv API down) → investigate + retry
   - Feature computation failures → manual rerun
   - Intel stage failures → escalate to on-call

### Troubleshooting

**Issue: Ingestion stuck or timing out**
```bash
# Check database
psql -c "SELECT COUNT(*) FROM documents WHERE DATE(ingested_at) = CURRENT_DATE;"

# Check logs
docker logs raidar-api 2>&1 | grep "ingest"

# Manually retry
python src/ingestion/run.py
```

**Issue: Low brief quality (LLM judge score < 0.70)**
- Review golden set — did we miss edge cases?
- Check if skeptic flags are helpful or noisy
- Consider tweaking skeptic checklist
- Escalate more documents to Fable

**Issue: High cost (> $1 per brief)**
- Check skeptic escalation rate (if > 50%, might be over-escalating)
- Verify Haiku is used for discovery, not Sonnet
- Check if synthesis is using Fable (expected, can't reduce)

**Issue: Topics not making sense**
- Sample 5 documents, manually review topic assignments
- If accuracy < 85%, retrain or adjust topic taxonomy
- Check if Haiku topic prompt needs tuning

### Scheduled Maintenance

**Weekly:**
- Review Langfuse dashboard for trends
- Check Pinecone vector count matches document count
- Spot-check 5 random briefs for quality

**Monthly:**
- Full eval harness run (not just daily sample)
- Review and update golden sets
- Tune model routing (can we move more to Haiku?)
- Audit LLM costs by stage

**Quarterly:**
- Major version upgrade of dependencies
- Security review of secrets management
- Performance optimization (caching, query tuning)
- User feedback session

---

## Part 6: Success Metrics

By end of Phase 6, aim for:

| Metric | Target | How to Measure |
|--------|--------|----------------|
| **Skeptic catch rate** | > 85% | Eval on golden set daily |
| **Brief faithfulness** | > 0.85 | LLM-as-judge score |
| **Cost per brief** | < $0.50 | Langfuse cost tracking |
| **Latency p99** | < 2 min | Langfuse latency |
| **Uptime** | > 99.5% | AWS CloudWatch |
| **User satisfaction** | > 4/5 | User survey monthly |
| **Feature coverage** | > 95% | Feature completion rate |

---

## Implementation Order

1. **Week 1:** Golden sets + evaluation harness (eval/metrics.py + eval/runner.py)
2. **Week 2:** Langfuse + mlflow setup, daily eval runs via Airflow
3. **Week 3:** Docker + Kubernetes manifests, CI/CD pipeline
4. **Week 4:** Staging deployment, pre-launch testing
5. **Week 5:** Production deployment, monitoring + runbook

---

## Resources

- **Langfuse docs:** https://docs.langfuse.com
- **mlflow docs:** https://mlflow.org/docs/latest/index.html
- **Kubernetes:** https://kubernetes.io/docs/home/
- **Airflow:** https://airflow.apache.org/docs/
- **Docker:** https://docs.docker.com/

---

## Questions?

See README.md for API docs, CLAUDE.md for project constraints, PROJECT_COMPLETE.md for architecture overview.

Good luck! 🚀
