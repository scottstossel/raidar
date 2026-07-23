import logging
import json
from anthropic import Anthropic
from src.config import settings
from src.monitoring.langfuse_tracer import tracer

logger = logging.getLogger(__name__)


class AnalysisAgent:
    """Signal extraction: what does this work claim, and why does it matter?"""

    def __init__(self):
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.prompt_template = self._load_prompt()

    def _load_prompt(self) -> str:
        """Load analysis prompt template."""
        try:
            with open("src/intel/prompts/analysis.txt") as f:
                return f.read()
        except FileNotFoundError:
            logger.warning("Analysis prompt not found; using fallback")
            return "Analyze: {title}\n{content}"

    def extract_signal(
        self,
        document_id: int,
        title: str,
        topic: str,
        content: str,
    ) -> dict:
        """
        Extract signal from a document.

        Returns:
            {
                "core_claim": str,
                "significance": str,
                "limitations": str,
                "applicability": str,
                "key_metrics": str,
                "document_id": int,
                "success": bool,
            }
        """
        prompt = self.prompt_template.format(
            title=title,
            topic=topic,
            content=content[:800],  # Limit content
        )

        try:
            response = self.client.messages.create(
                model=settings.model_analysis,
                max_tokens=400,
                messages=[{"role": "user", "content": prompt}],
            )

            result_text = response.content[0].text
            result = self._parse_response(result_text, document_id)

            # Log to Langfuse
            tracer.trace_agent_decision(
                agent_name="analysis",
                input_data=f"{title}",
                decision=result.get("core_claim", "extraction failed"),
                metadata={
                    "document_id": document_id,
                    "applicability": result.get("applicability"),
                },
            )

            return result

        except Exception as e:
            logger.error(f"Analysis extraction failed for {document_id}: {e}")
            return {
                "core_claim": "",
                "significance": "",
                "limitations": "",
                "applicability": "unknown",
                "key_metrics": "",
                "document_id": document_id,
                "success": False,
            }

    def _parse_response(self, response_text: str, document_id: int) -> dict:
        """Parse JSON response from LLM."""
        default = {
            "core_claim": "",
            "significance": "",
            "limitations": "",
            "applicability": "unknown",
            "key_metrics": "",
            "document_id": document_id,
            "success": False,
        }

        try:
            # Extract JSON from response (LLM might add extra text)
            json_start = response_text.find("{")
            json_end = response_text.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                parsed = json.loads(json_str)
                parsed["document_id"] = document_id
                parsed["success"] = True
                return parsed
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug(f"Failed to parse analysis JSON: {e}")

        return default
