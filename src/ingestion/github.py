import logging
import httpx
from datetime import datetime, timedelta
from typing import List, Optional
from src.ingestion.models import Document
from src.ingestion.base import FetchError, log_fetch_attempt
from src.config import settings

logger = logging.getLogger(__name__)

# GitHub API: https://docs.github.com/en/rest
GITHUB_API_BASE = "https://api.github.com"


class GitHubAdapter:
    """Fetch trending AI/ML repositories and discussions from GitHub."""

    def __init__(self, token: str = ""):
        self.token = token or settings.github_token
        self.client = httpx.Client(
            timeout=30.0,
            headers={"Authorization": f"token {self.token}"} if self.token else {},
        )

    def fetch_recent_repos(self, num_repos: int = 50) -> List[Document]:
        """Fetch recently starred/active AI/ML repositories."""
        documents = []

        # Search for repos with topics related to AI/ML, updated in last 7 days
        look_back_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        query = (
            f"topic:machine-learning OR topic:deep-learning OR topic:llm "
            f"OR topic:artificial-intelligence pushed:>{look_back_date}"
        )

        try:
            docs = self._search_repos(query, num_repos)
            documents.extend(docs)
            logger.info(f"Fetched {len(docs)} repos from GitHub search")
        except FetchError as e:
            logger.error(f"Failed to fetch repos from GitHub: {e}")

        return documents

    def fetch_trending_discussions(self, num_discussions: int = 30) -> List[Document]:
        """Fetch trending discussions about AI/ML."""
        documents = []

        # GitHub discussions API requires different endpoint
        # This is a simplified version; real implementation would need GraphQL
        look_back_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        query = (
            f"is:discussion label:machine-learning OR label:ai "
            f"updated:>{look_back_date}"
        )

        try:
            docs = self._search_issues_discussions(query, num_discussions, is_discussion=True)
            documents.extend(docs)
            logger.info(f"Fetched {len(docs)} discussions from GitHub")
        except FetchError as e:
            logger.error(f"Failed to fetch discussions: {e}")

        return documents

    def _search_repos(self, query: str, max_results: int) -> List[Document]:
        """Search GitHub repos using search API."""
        url = f"{GITHUB_API_BASE}/search/repositories"
        params = {
            "q": query,
            "sort": "updated",
            "order": "desc",
            "per_page": min(max_results, 100),
        }

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except httpx.RequestError as e:
            raise FetchError(f"Failed to search GitHub repos: {e}")

        return self._parse_repos(data.get("items", []))

    def _search_issues_discussions(
        self, query: str, max_results: int, is_discussion: bool = False
    ) -> List[Document]:
        """Search GitHub issues/discussions."""
        url = f"{GITHUB_API_BASE}/search/issues"
        params = {
            "q": query,
            "sort": "updated",
            "order": "desc",
            "per_page": min(max_results, 100),
        }

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except httpx.RequestError as e:
            raise FetchError(f"Failed to search GitHub issues/discussions: {e}")

        return self._parse_issues(data.get("items", []), is_discussion=is_discussion)

    def _parse_repos(self, repos: list) -> List[Document]:
        """Parse repo search results into Documents."""
        documents = []

        for repo in repos:
            try:
                doc = Document(
                    source="github",
                    source_id=f"repo/{repo['full_name']}",
                    title=repo["name"],
                    content=repo.get("description", "") or "No description provided.",
                    url=repo["html_url"],
                    metadata={
                        "owner": repo["owner"]["login"],
                        "stars": repo["stargazers_count"],
                        "language": repo.get("language"),
                        "topics": repo.get("topics", []),
                    },
                    fetched_at=datetime.fromisoformat(
                        repo["pushed_at"].replace("Z", "+00:00")
                    ),
                )
                documents.append(doc)
                log_fetch_attempt("github", repo["full_name"], True)
            except Exception as e:
                logger.warning(f"Failed to parse GitHub repo {repo.get('full_name')}: {e}")
                continue

        return documents

    def _parse_issues(self, issues: list, is_discussion: bool = False) -> List[Document]:
        """Parse issue/discussion search results into Documents."""
        documents = []
        source_type = "github_discussion" if is_discussion else "github_issue"

        for issue in issues:
            try:
                doc = Document(
                    source=source_type,
                    source_id=f"issue/{issue['number']}",
                    title=issue["title"],
                    content=issue.get("body", "") or "No description provided.",
                    url=issue["html_url"],
                    metadata={
                        "author": issue["user"]["login"],
                        "comments": issue["comments"],
                        "state": issue["state"],
                        "labels": [label["name"] for label in issue.get("labels", [])],
                    },
                    fetched_at=datetime.fromisoformat(
                        issue["updated_at"].replace("Z", "+00:00")
                    ),
                )
                documents.append(doc)
                log_fetch_attempt(source_type, str(issue["number"]), True)
            except Exception as e:
                logger.warning(f"Failed to parse GitHub issue {issue.get('number')}: {e}")
                continue

        return documents

    def close(self):
        """Close HTTP client."""
        self.client.close()
