import logging
import json
from anthropic import Anthropic
from src.config import settings
from src.monitoring.langfuse_tracer import tracer

logger = logging.getLogger(__name__)


class DiscoveryAgent:
    """Relevance filtering: determines if documents merit inclusion in the brief."""

    def __init__(self):
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.prompt_template = self._load_prompt()

    def _load_prompt(self) -> str:
        """Load discovery prompt template."""
        try:
            with open("src/intel/prompts/discovery.txt") as f:
                return f.read()
        except FileNotFoundError:
            logger.warning("Discovery prompt not found; using fallback")
            return "{title}\n{topic}\n{trend_score}"

    def evaluate_relevance(
        self,
        document_id: int,
        title: str,
        source: str,
        topic: str,
        theme: str,
        trend_score: float,
        content: str,
    ) -> dict:
        """
        Evaluate if a document is relevant for the brief.

        Returns:
            {
                "relevant": bool,
                "confidence": 0.0-1.0,
                "reasoning": str,
                "priority": "high" | "medium" | "low" (if relevant),
                "document_id": int,
            }
        """
        prompt = self.prompt_template.format(
            title=title,
            source=source,
            topic=topic,
            theme=theme,
            trend_score=trend_score,
            content=content[:500],  # Limit content length
        )

        try:
            response = self.client.messages.create(
                model=settings.model_discovery,
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )

            result_text = response.content[0].text
            result = self._parse_response(result_text, document_id)

            # Log to Langfuse
            tracer.trace_agent_decision(
                agent_name="discovery",
                input_data=f"{title} ({source})",
                decision="relevant" if result["relevant"] else "not relevant",
                confidence=result["confidence"],
                metadata={"document_id": document_id, "priority": result.get("priority")},
            )

            return result

        except Exception as e:
            logger.error(f"Discovery evaluation failed for {document_id}: {e}")
            return {
                "relevant": False,
                "confidence": 0.0,
                "reasoning": f"Error: {str(e)}",
                "document_id": document_id,
            }

    def _parse_response(self, response_text: str, document_id: int) -> dict:
        """Parse LLM response."""
        result = {
            "relevant": False,
            "confidence": 0.5,
            "reasoning": "",
            "document_id": document_id,
        }

        lines = response_text.strip().split("\n")
        for line in lines:
            if line.startswith("RELEVANT:"):
                result["relevant"] = "yes" in line.lower()
            elif line.startswith("CONFIDENCE:"):
                try:
                    result["confidence"] = float(line.split(":")[-1].strip())
                except ValueError:
                    result["confidence"] = 0.5
            elif line.startswith("REASONING:"):
                result["reasoning"] = line.split(":", 1)[-1].strip()
            elif line.startswith("PRIORITY:"):
                priority = line.split(":")[-1].strip().lower()
                if priority in ("high", "medium", "low"):
                    result["priority"] = priority

        return result
