"""Unit tests for theme assignment."""

from src.features.themes import ThemeAssigner


def test_theme_assigner_maps_topics():
    """Test that known topics map to themes."""
    assigner = ThemeAssigner()

    theme = assigner.assign_theme(
        "LLM Fine-tuning",
        "A paper about fine-tuning language models.",
        "Large Language Models"
    )

    assert theme == "Foundation Models & LLMs"


def test_theme_assigner_uses_keyword_fallback():
    """Test that keywords trigger themes."""
    assigner = ThemeAssigner()

    theme = assigner.assign_theme(
        "Inference Optimization",
        "This work optimizes model inference for edge devices.",
        "Unknown Topic"
    )

    # "inference" keyword should trigger the theme
    assert theme == "Inference & Efficiency"


def test_theme_assigner_defaults_for_unknown():
    """Test default theme for unknown topics and no keywords."""
    assigner = ThemeAssigner()

    theme = assigner.assign_theme(
        "Unknown Title",
        "Some random content.",
        "Unknown Topic"
    )

    assert theme == "Miscellaneous AI Research"
