import logging
import httpx
from datetime import datetime, timedelta
from typing import List
from src.ingestion.models import Document
from src.ingestion.base import FetchError, log_fetch_attempt
from src.config import settings

logger = logging.getLogger(__name__)

# Hugging Face API: https://huggingface.co/docs/hub/api
HF_API_BASE = "https://huggingface.co/api"


class HuggingFaceAdapter:
    """Fetch trending models and datasets from Hugging Face Hub."""

    def __init__(self):
        self.client = httpx.Client(timeout=30.0)

    def fetch_recent_models(self, num_models: int = 50) -> List[Document]:
        """Fetch recently updated models (papers, demos, state-of-art)."""
        documents = []

        try:
            # Fetch models sorted by recent activity
            docs = self._search_models(num_models)
            documents.extend(docs)
            logger.info(f"Fetched {len(docs)} models from Hugging Face")
        except FetchError as e:
            logger.error(f"Failed to fetch models from Hugging Face: {e}")

        return documents

    def fetch_recent_datasets(self, num_datasets: int = 30) -> List[Document]:
        """Fetch recently updated datasets."""
        documents = []

        try:
            docs = self._search_datasets(num_datasets)
            documents.extend(docs)
            logger.info(f"Fetched {len(docs)} datasets from Hugging Face")
        except FetchError as e:
            logger.error(f"Failed to fetch datasets from Hugging Face: {e}")

        return documents

    def _search_models(self, max_results: int) -> List[Document]:
        """Search for models on Hugging Face Hub."""
        url = f"{HF_API_BASE}/models"
        params = {
            "limit": min(max_results, 100),
            "sort": "lastModified",
            "direction": -1,
        }

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except httpx.RequestError as e:
            raise FetchError(f"Failed to search Hugging Face models: {e}")

        return self._parse_models(data if isinstance(data, list) else data.get("items", []))

    def _search_datasets(self, max_results: int) -> List[Document]:
        """Search for datasets on Hugging Face Hub."""
        url = f"{HF_API_BASE}/datasets"
        params = {
            "limit": min(max_results, 100),
            "sort": "lastModified",
            "direction": -1,
        }

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except httpx.RequestError as e:
            raise FetchError(f"Failed to search Hugging Face datasets: {e}")

        return self._parse_datasets(data if isinstance(data, list) else data.get("items", []))

    def _parse_models(self, models: list) -> List[Document]:
        """Parse model entries into Documents."""
        documents = []

        for model in models:
            try:
                # Filter to models updated recently
                if isinstance(model, dict) and "lastModified" in model:
                    last_modified = datetime.fromisoformat(
                        model["lastModified"].replace("Z", "+00:00")
                    )
                    # Skip if older than 7 days
                    if datetime.now(last_modified.tzinfo) - last_modified > timedelta(days=7):
                        continue

                model_id = model.get("id") or model.get("name", "unknown")
                description = (
                    model.get("description") or model.get("summary") or "No description."
                )

                doc = Document(
                    source="huggingface",
                    source_id=f"model/{model_id}",
                    title=model_id,
                    content=description,
                    url=f"https://huggingface.co/{model_id}",
                    metadata={
                        "downloads": model.get("downloads", 0),
                        "likes": model.get("likes", 0),
                        "tags": model.get("tags", []),
                        "pipeline_tag": model.get("pipeline_tag"),
                    },
                    fetched_at=datetime.fromisoformat(
                        model.get("lastModified", datetime.utcnow().isoformat()).replace(
                            "Z", "+00:00"
                        )
                    ),
                )
                documents.append(doc)
                log_fetch_attempt("huggingface", model_id, True)
            except Exception as e:
                logger.warning(f"Failed to parse HF model {model.get('id')}: {e}")
                continue

        return documents

    def _parse_datasets(self, datasets: list) -> List[Document]:
        """Parse dataset entries into Documents."""
        documents = []

        for dataset in datasets:
            try:
                # Filter to datasets updated recently
                if isinstance(dataset, dict) and "lastModified" in dataset:
                    last_modified = datetime.fromisoformat(
                        dataset["lastModified"].replace("Z", "+00:00")
                    )
                    if datetime.now(last_modified.tzinfo) - last_modified > timedelta(days=7):
                        continue

                dataset_id = dataset.get("id") or dataset.get("name", "unknown")
                description = (
                    dataset.get("description") or dataset.get("summary") or "No description."
                )

                doc = Document(
                    source="huggingface_dataset",
                    source_id=f"dataset/{dataset_id}",
                    title=dataset_id,
                    content=description,
                    url=f"https://huggingface.co/datasets/{dataset_id}",
                    metadata={
                        "downloads": dataset.get("downloads", 0),
                        "likes": dataset.get("likes", 0),
                        "tags": dataset.get("tags", []),
                    },
                    fetched_at=datetime.fromisoformat(
                        dataset.get("lastModified", datetime.utcnow().isoformat()).replace(
                            "Z", "+00:00"
                        )
                    ),
                )
                documents.append(doc)
                log_fetch_attempt("huggingface_dataset", dataset_id, True)
            except Exception as e:
                logger.warning(f"Failed to parse HF dataset {dataset.get('id')}: {e}")
                continue

        return documents

    def close(self):
        """Close HTTP client."""
        self.client.close()
