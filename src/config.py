from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    data_dir: str = os.getenv("DATA_DIR", "data")
    arxiv_max_results: int = int(os.getenv("ARXIV_MAX_RESULTS", "50"))
    github_token: str = os.getenv("GITHUB_TOKEN", "")
    github_search_query: str = os.getenv(
        "GITHUB_SEARCH_QUERY", 
        "llm evaluation OR ai safety OR ai agents"
    )
    github_max_results: int = int(os.getenv("GITHUB_MAX_RESULTS", "30"))