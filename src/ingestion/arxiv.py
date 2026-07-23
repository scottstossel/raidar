import logging
import httpx
from datetime import datetime, timedelta
from typing import List
from xml.etree import ElementTree as ET
import time

from src.ingestion.models import Document
from src.ingestion.base import FetchError, log_fetch_attempt
from src.config import settings

logger = logging.getLogger(__name__)

# arXiv API: https://arxiv.org/help/api/user-manual
ARXIV_API_BASE = "http://export.arxiv.org/api/query"


class ArxivAdapter:
    """Fetch papers from arXiv API. Respects rate limits (1 req/3 sec)."""

    def __init__(self, email: str = ""):
        self.email = email or settings.arxiv_email
        self.client = httpx.Client(timeout=30.0)
        self.last_request_time = 0

    def _rate_limit_wait(self):
        """Ensure we don't exceed arXiv's 1 request per 3 seconds limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < 3:
            time.sleep(3 - elapsed)

    def fetch_recent(self, num_papers: int = 100) -> List[Document]:
        """Fetch recent papers from arXiv (e.g., from last 7 days)."""
        documents = []

        # Search for papers updated in the last 7 days across AI/ML categories
        categories = ["cs.AI", "cs.LG", "cs.NE"]  # AI, ML, Neural Networks
        look_back_days = 7

        for cat in categories:
            try:
                docs = self._search_category(cat, look_back_days, num_papers)
                documents.extend(docs)
                logger.info(f"Fetched {len(docs)} papers from {cat}")
            except FetchError as e:
                logger.error(f"Failed to fetch from {cat}: {e}")
                continue

        return documents

    def _search_category(
        self, category: str, look_back_days: int, max_results: int
    ) -> List[Document]:
        """Search arXiv for papers in a category updated recently."""
        self._rate_limit_wait()

        # Date filter: papers updated in the last N days
        cutoff_date = (datetime.utcnow() - timedelta(days=look_back_days)).strftime(
            "%Y%m%d%H%M%S"
        )
        query = f'cat:{category} AND submittedDate:[{cutoff_date}000000 TO 9999999999999999]'

        params = {
            "search_query": query,
            "start": 0,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }

        try:
            response = self.client.get(ARXIV_API_BASE, params=params)
            response.raise_for_status()
        except httpx.RequestError as e:
            raise FetchError(f"Failed to fetch from arXiv: {e}")

        self.last_request_time = time.time()

        return self._parse_feed(response.text)

    def _parse_feed(self, xml_text: str) -> List[Document]:
        """Parse arXiv Atom feed and extract documents."""
        documents = []

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            raise FetchError(f"Invalid XML response: {e}")

        # arXiv uses Atom namespace
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        for entry in root.findall("atom:entry", ns):
            try:
                arxiv_id = entry.findtext("atom:id", namespace=ns["atom"]).split("/abs/")[-1]
                title = entry.findtext("atom:title", namespace=ns["atom"])
                summary = entry.findtext("atom:summary", namespace=ns["atom"])
                published = entry.findtext("atom:published", namespace=ns["atom"])

                # Extract authors for metadata
                authors = [
                    author.findtext("atom:name", namespace=ns["atom"])
                    for author in entry.findall("atom:author", ns)
                ]

                url = f"https://arxiv.org/abs/{arxiv_id}"

                doc = Document(
                    source="arxiv",
                    source_id=arxiv_id,
                    title=title.strip(),
                    content=summary.strip(),
                    url=url,
                    metadata={"authors": authors},
                    fetched_at=datetime.fromisoformat(published.replace("Z", "+00:00")),
                )

                documents.append(doc)
                log_fetch_attempt("arxiv", arxiv_id, True)

            except Exception as e:
                logger.warning(f"Failed to parse arXiv entry: {e}")
                continue

        return documents

    def close(self):
        """Close HTTP client."""
        self.client.close()
