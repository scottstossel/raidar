from pydantic_settings import BaseSettings
from typing import Literal

class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql://raidar:raidar@localhost:5432/raidar"

    # LLM API keys
    anthropic_api_key: str = ""
    cohere_api_key: str = ""

    # Pinecone
    pinecone_api_key: str = ""
    pinecone_environment: str = "us-east-1-aws"
    pinecone_index_name: str = "raidar"

    # Monitoring
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    mlflow_tracking_uri: str = "http://localhost:5000"

    # Sources
    github_token: str = ""
    arxiv_email: str = ""

    # Environment
    environment: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"

    # Model routing
    model_topic_tagging: str = "claude-haiku-4-5-20251001"
    model_discovery: str = "claude-sonnet-5"
    model_analysis: str = "claude-sonnet-5"
    model_skeptic_default: str = "claude-sonnet-5"
    model_skeptic_escalated: str = "claude-fable-5"
    model_synthesis: str = "claude-fable-5"
    model_judge: str = "claude-opus-4-8"

    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()
