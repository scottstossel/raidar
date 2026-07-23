import logging
from typing import Optional
from src.intel.agents.discovery import DiscoveryAgent
from src.intel.agents.analysis import AnalysisAgent
from src.intel.agents.skeptic import SkepticAgent
from src.intel.agents.synthesis import SynthesisAgent
from src.db.session import get_db_session
from src.monitoring.langfuse_tracer import tracer

logger = logging.getLogger(__name__)


class IntelOrchestrator:
    """Coordinates the multi-agent pipeline: discovery → analysis → skeptic → synthesis."""

    def __init__(self):
        self.discovery = DiscoveryAgent()
        self.analysis = AnalysisAgent()
        self.skeptic = SkepticAgent()
        self.synthesis = SynthesisAgent()
        self.db = get_db_session()

    def process_documents(
        self,
        documents: list[dict],
        brief_type: str = "daily",
    ) -> dict:
        """
        Process a batch of documents through the intel pipeline.

        Args:
            documents: List of dicts with document_id, title, source, topic, theme, trend_score, content
            brief_type: "daily" or "topic:<topic_name>"

        Returns:
            {
                "brief": {content, brief_type, themes, ...},
                "processed_count": int,
                "passed_discovery": int,
                "skeptic_escalations": int,
            }
        """
        logger.info(f"Processing {len(documents)} documents for {brief_type} brief")

        relevant_documents = []
        escalation_count = 0

        for doc in documents:
            try:
                # Stage 1: Discovery (relevance filtering)
                discovery_result = self.discovery.evaluate_relevance(
                    document_id=doc.get("document_id", -1),
                    title=doc.get("title", ""),
                    source=doc.get("source", ""),
                    topic=doc.get("topic", ""),
                    theme=doc.get("theme", ""),
                    trend_score=doc.get("trend_score", 0.0),
                    content=doc.get("content", ""),
                )

                if not discovery_result.get("relevant"):
                    logger.debug(f"Document {doc.get('document_id')} filtered by discovery")
                    continue

                logger.info(
                    f"Document {doc.get('document_id')} passed discovery "
                    f"(confidence: {discovery_result['confidence']:.2f})"
                )

                # Stage 2: Analysis (signal extraction)
                analysis_result = self.analysis.extract_signal(
                    document_id=doc.get("document_id", -1),
                    title=doc.get("title", ""),
                    topic=doc.get("topic", ""),
                    content=doc.get("content", ""),
                )

                # Stage 3: Skeptic (claim verification)
                skeptic_result = self.skeptic.verify_claims(
                    document_id=doc.get("document_id", -1),
                    title=doc.get("title", ""),
                    source=doc.get("source", ""),
                    core_claim=analysis_result.get("core_claim", ""),
                    content=doc.get("content", ""),
                    use_escalated=False,  # Start with default Sonnet
                )

                # If skeptic flags high-confidence issues, escalate to Fable
                if skeptic_result.get("escalation_needed"):
                    logger.info(
                        f"Escalating document {doc.get('document_id')} to Fable "
                        f"({len(skeptic_result['flags'])} flags)"
                    )
                    skeptic_result = self.skeptic.verify_claims(
                        document_id=doc.get("document_id", -1),
                        title=doc.get("title", ""),
                        source=doc.get("source", ""),
                        core_claim=analysis_result.get("core_claim", ""),
                        content=doc.get("content", ""),
                        use_escalated=True,
                    )
                    escalation_count += 1

                # Combine results
                enriched_doc = {
                    **doc,
                    **discovery_result,
                    **analysis_result,
                    **skeptic_result,
                }

                relevant_documents.append(enriched_doc)

            except Exception as e:
                logger.error(f"Failed to process document {doc.get('document_id')}: {e}")
                continue

        logger.info(
            f"Processed {len(documents)} documents; {len(relevant_documents)} passed discovery; "
            f"{escalation_count} escalated to Fable"
        )

        # Stage 4: Synthesis (brief generation)
        brief_result = self.synthesis.generate_brief(relevant_documents, brief_type)

        return {
            "brief": brief_result,
            "processed_count": len(documents),
            "passed_discovery": len(relevant_documents),
            "skeptic_escalations": escalation_count,
        }

    def close(self):
        """Clean up resources."""
        self.db.close()
        tracer.flush()
