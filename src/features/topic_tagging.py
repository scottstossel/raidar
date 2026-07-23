import logging
from anthropic import Anthropic
from src.config import settings
from src.features.models import TopicTaggingResult
from src.monitoring.langfuse_tracer import tracer

logger = logging.getLogger(__name__)

# Topic taxonomy (can be extended)
TOPIC_TAXONOMY = {
    "Large Language Models": {
        "keywords": ["llm", "language model", "gpt", "bert", "transformer"],
        "secondary": ["prompt engineering", "fine-tuning", "alignment"],
    },
    "Computer Vision": {
        "keywords": ["vision", "image", "object detection", "segmentation", "diffusion"],
        "secondary": ["image generation", "video", "3d"],
    },
    "Multimodal Models": {
        "keywords": ["multimodal", "vision-language", "clip", "dall-e"],
        "secondary": ["text-to-image", "visual reasoning"],
    },
    "Reinforcement Learning": {
        "keywords": ["reinforcement learning", "rl", "policy", "reward"],
        "secondary": ["q-learning", "actor-critic", "imitation learning"],
    },
    "Knowledge Representation": {
        "keywords": ["knowledge graph", "ontology", "semantic", "reasoning"],
        "secondary": ["graph neural network", "entity linking"],
    },
    "AI Safety & Ethics": {
        "keywords": ["safety", "alignment", "bias", "fairness", "interpretability"],
        "secondary": ["adversarial", "robustness", "explainability"],
    },
}


class TopicTagger:
    """Classify documents into topics using Haiku."""

    def __init__(self):
        self.client = Anthropic(api_key=settings.anthropic_api_key)

    def tag_document(self, title: str, content: str) -> TopicTaggingResult:
        """Classify a document into primary and secondary topics."""
        prompt = self._build_prompt(title, content)

        try:
            response = self.client.messages.create(
                model=settings.model_topic_tagging,
                max_tokens=200,
                messages=[
                    {
                        "role": "user",
                        "content": prompt,
                    }
                ],
            )

            result_text = response.content[0].text
            result = self._parse_response(result_text)

            # Log to Langfuse
            tracer.trace_llm_call(
                name="topic_tagging",
                model=settings.model_topic_tagging,
                prompt=prompt,
                response=result_text,
                metadata={
                    "primary_topic": result.primary_topic,
                    "confidence": result.confidence,
                },
            )

            return result

        except Exception as e:
            logger.error(f"Failed to tag document: {e}")
            return TopicTaggingResult(
                primary_topic="Unknown",
                secondary_topics=[],
                confidence=0.0,
            )

    def _build_prompt(self, title: str, content: str) -> str:
        """Build prompt for topic tagging."""
        topics_str = ", ".join(TOPIC_TAXONOMY.keys())
        return f"""Classify the following research document into one primary topic and up to two secondary topics.

Available topics: {topics_str}

Document title: {title}

Document content (abstract/summary):
{content}

Respond in this exact format:
PRIMARY_TOPIC: <topic name>
CONFIDENCE: <0.0-1.0>
SECONDARY_TOPICS: <comma-separated list or "none">

Be concise and use only topics from the available list."""

    def _parse_response(self, response_text: str) -> TopicTaggingResult:
        """Parse LLM response into TopicTaggingResult."""
        lines = response_text.strip().split("\n")
        primary = "Unknown"
        confidence = 0.5
        secondary = []

        for line in lines:
            if line.startswith("PRIMARY_TOPIC:"):
                primary = line.replace("PRIMARY_TOPIC:", "").strip()
            elif line.startswith("CONFIDENCE:"):
                try:
                    confidence = float(line.replace("CONFIDENCE:", "").strip())
                except ValueError:
                    confidence = 0.5
            elif line.startswith("SECONDARY_TOPICS:"):
                topics_str = line.replace("SECONDARY_TOPICS:", "").strip()
                if topics_str.lower() != "none":
                    secondary = [t.strip() for t in topics_str.split(",")]

        return TopicTaggingResult(
            primary_topic=primary,
            secondary_topics=secondary,
            confidence=min(1.0, max(0.0, confidence)),
        )
