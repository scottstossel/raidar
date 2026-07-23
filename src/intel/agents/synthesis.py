import logging
from datetime import datetime
from anthropic import Anthropic
from src.config import settings
from src.monitoring.langfuse_tracer import tracer

logger = logging.getLogger(__name__)


class SynthesisAgent:
    """Brief generation: write the final research narrative."""

    def __init__(self):
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.prompt_template = self._load_prompt()

    def _load_prompt(self) -> str:
        """Load synthesis prompt template."""
        try:
            with open("src/intel/prompts/synthesis.txt") as f:
                return f.read()
        except FileNotFoundError:
            logger.warning("Synthesis prompt not found; using fallback")
            return "Write a brief from: {documents_summary}"

    def generate_brief(
        self,
        documents_with_signal: list[dict],
        brief_type: str = "daily",
    ) -> dict:
        """
        Generate a research brief from curated documents.

        Args:
            documents_with_signal: List of dicts with title, topic, analysis, skeptic flags
            brief_type: "daily" or "topic:<topic_name>"

        Returns:
            {
                "content": str (the brief text),
                "brief_type": str,
                "generated_at": datetime,
                "document_count": int,
                "themes": list[str],
                "success": bool,
            }
        """
        if not documents_with_signal:
            logger.warning("No documents provided for synthesis")
            return {
                "content": "No documents passed relevance screening for today's brief.",
                "brief_type": brief_type,
                "generated_at": datetime.utcnow(),
                "document_count": 0,
                "themes": [],
                "success": False,
            }

        # Summarize documents for the prompt
        summary = self._summarize_documents(documents_with_signal)

        prompt = self.prompt_template.format(
            documents_summary=summary,
            date=datetime.utcnow().strftime("%Y-%m-%d"),
        )

        try:
            response = self.client.messages.create(
                model=settings.model_synthesis,
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            )

            brief_text = response.content[0].text

            # Log to Langfuse
            tracer.trace_llm_call(
                name="synthesis",
                model=settings.model_synthesis,
                prompt=prompt[:300],  # Log first 300 chars of prompt
                response=brief_text[:300],
                metadata={
                    "brief_type": brief_type,
                    "document_count": len(documents_with_signal),
                },
            )

            # Extract themes from content (simple heuristic)
            themes = self._extract_themes(brief_text)

            return {
                "content": brief_text,
                "brief_type": brief_type,
                "generated_at": datetime.utcnow(),
                "document_count": len(documents_with_signal),
                "themes": themes,
                "success": True,
            }

        except Exception as e:
            logger.error(f"Brief generation failed: {e}")
            return {
                "content": f"Brief generation failed: {str(e)}",
                "brief_type": brief_type,
                "generated_at": datetime.utcnow(),
                "document_count": len(documents_with_signal),
                "themes": [],
                "success": False,
            }

    def _summarize_documents(self, documents: list[dict]) -> str:
        """Create a summary of documents for the synthesis prompt."""
        summary_lines = []
        for i, doc in enumerate(documents[:20], 1):  # Limit to top 20
            summary_lines.append(
                f"{i}. **{doc.get('title', 'Untitled')}** ({doc.get('topic', 'unknown')})\n"
                f"   - {doc.get('core_claim', 'No claim')}\n"
                f"   - Significance: {doc.get('significance', 'Unknown')}"
            )
            if doc.get('flags'):
                summary_lines.append(f"   - ⚠️ Flags: {', '.join(doc['flags'])}")

        return "\n".join(summary_lines)

    def _extract_themes(self, brief_text: str) -> list[str]:
        """Extract theme names from the brief (simple heuristic)."""
        themes = []
        for line in brief_text.split("\n"):
            if line.startswith("## ") and "Theme" not in line:
                theme = line.replace("##", "").strip()
                if theme and len(theme) < 100:
                    themes.append(theme)
        return themes[:5]  # Return up to 5 themes
