import logging
from anthropic import Anthropic
from src.config import settings
from src.monitoring.langfuse_tracer import tracer

logger = logging.getLogger(__name__)


class SkepticAgent:
    """Claim verification: does this claim hold up to scrutiny?"""

    def __init__(self):
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.prompt_template = self._load_prompt()

    def _load_prompt(self) -> str:
        """Load skeptic checklist prompt."""
        try:
            with open("src/intel/prompts/skeptic_checklist.txt") as f:
                return f.read()
        except FileNotFoundError:
            logger.warning("Skeptic prompt not found; using fallback")
            return "Verify: {title}\nClaim: {core_claim}"

    def verify_claims(
        self,
        document_id: int,
        title: str,
        source: str,
        core_claim: str,
        content: str,
        use_escalated: bool = False,
    ) -> dict:
        """
        Verify claims against the checklist.

        Args:
            document_id: Document ID
            title: Document title
            source: Document source (arxiv, github, etc.)
            core_claim: Extracted core claim from analysis agent
            content: Document content
            use_escalated: If True, use Fable (more expensive); else Sonnet

        Returns:
            {
                "flags": list of issues found,
                "source_match": "ok" | "concern" | issue,
                "benchmark_comparison": "ok" | "concern" | issue,
                "publication_status": str,
                "adoption_signals": "ok" | "concern" | issue,
                "reproducibility": "ok" | "concern" | issue,
                "document_id": int,
                "escalation_needed": bool,
            }
        """
        prompt = self.prompt_template.format(
            title=title,
            source=source,
            core_claim=core_claim,
            content=content[:600],
        )

        model = settings.model_skeptic_escalated if use_escalated else settings.model_skeptic_default

        try:
            response = self.client.messages.create(
                model=model,
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )

            result_text = response.content[0].text
            result = self._parse_response(result_text, document_id)

            # Log to Langfuse
            tracer.trace_agent_decision(
                agent_name="skeptic",
                input_data=f"{title}",
                decision="concerns found" if result["flags"] else "no issues",
                metadata={
                    "document_id": document_id,
                    "flags": result["flags"],
                    "escalated": use_escalated,
                },
            )

            return result

        except Exception as e:
            logger.error(f"Skeptic verification failed for {document_id}: {e}")
            return {
                "flags": ["error"],
                "source_match": "error",
                "benchmark_comparison": "error",
                "publication_status": "unknown",
                "adoption_signals": "error",
                "reproducibility": "error",
                "document_id": document_id,
                "escalation_needed": True,
            }

    def _parse_response(self, response_text: str, document_id: int) -> dict:
        """Parse skeptic checklist response."""
        result = {
            "flags": [],
            "source_match": "ok",
            "benchmark_comparison": "ok",
            "publication_status": "unknown",
            "adoption_signals": "ok",
            "reproducibility": "ok",
            "document_id": document_id,
            "escalation_needed": False,
        }

        lines = response_text.strip().split("\n")
        for line in lines:
            if line.startswith("SOURCE_MATCH:"):
                val = line.split(":", 1)[-1].strip()
                result["source_match"] = val
                if "concern" in val.lower() or val not in ("ok", "concern"):
                    result["flags"].append("source_mismatch")
            elif line.startswith("BENCHMARK_COMPARISON:"):
                val = line.split(":", 1)[-1].strip()
                result["benchmark_comparison"] = val
                if "concern" in val.lower() or val not in ("ok", "concern"):
                    result["flags"].append("benchmark_unfair")
            elif line.startswith("PUBLICATION_STATUS:"):
                result["publication_status"] = line.split(":", 1)[-1].strip()
                if "preprint" in result["publication_status"].lower():
                    result["flags"].append("preprint_only")
            elif line.startswith("ADOPTION_SIGNALS:"):
                val = line.split(":", 1)[-1].strip()
                result["adoption_signals"] = val
                if "concern" in val.lower() or val not in ("ok", "concern"):
                    result["flags"].append("adoption_unverified")
            elif line.startswith("REPRODUCIBILITY:"):
                val = line.split(":", 1)[-1].strip()
                result["reproducibility"] = val
                if "concern" in val.lower() or val not in ("ok", "concern"):
                    result["flags"].append("reproducibility_concern")
            elif line.startswith("OVERALL_FLAGS:"):
                flags_str = line.split(":", 1)[-1].strip()
                if flags_str.lower() != "none":
                    result["flags"].extend([f.strip() for f in flags_str.split(",")])

        # Escalate if multiple issues found
        result["escalation_needed"] = len(result["flags"]) >= 2

        return result
