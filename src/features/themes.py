import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Heuristic theme mapping based on topics and content signals
THEME_MAPPING = {
    "Large Language Models": "Foundation Models & LLMs",
    "Multimodal Models": "Foundation Models & LLMs",
    "Computer Vision": "Perception & Recognition",
    "Reinforcement Learning": "Learning Methods & Theory",
    "Knowledge Representation": "Knowledge & Reasoning",
    "AI Safety & Ethics": "Safety, Ethics & Alignment",
}

# Additional heuristics: keywords that trigger themes
KEYWORD_THEMES = {
    "agent": "Agents & Autonomous Systems",
    "autonomous": "Agents & Autonomous Systems",
    "robotics": "Embodied AI & Robotics",
    "inference": "Inference & Efficiency",
    "optimization": "Learning Methods & Theory",
    "fine-tuning": "Foundation Models & LLMs",
    "prompt": "Foundation Models & LLMs",
}


class ThemeAssigner:
    """Assign high-level themes to documents."""

    def assign_theme(self, title: str, content: str, primary_topic: str) -> str:
        """
        Assign a theme based on topic and content signals.

        Args:
            title: Document title
            content: Document content
            primary_topic: Primary topic from topic tagger

        Returns:
            Theme name
        """
        # First, try direct mapping from topic
        if primary_topic in THEME_MAPPING:
            return THEME_MAPPING[primary_topic]

        # Fall back to keyword detection
        combined_text = (title + " " + content).lower()
        for keyword, theme in KEYWORD_THEMES.items():
            if keyword in combined_text:
                return theme

        # Default theme
        return "Miscellaneous AI Research"
