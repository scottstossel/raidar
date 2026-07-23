import logging
from typing import List
import httpx
from src.config import settings
from src.vector.pinecone_client import PineconeClient

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Compute embeddings and persist to Pinecone."""

    def __init__(self, model: str = "embed-english-v3.0"):
        self.model = model
        self.client = httpx.Client(timeout=30.0)
        self.pinecone = PineconeClient()
        self.api_key = settings.cohere_api_key

    def embed_document(self, text: str, doc_id: str, metadata: dict) -> str:
        """
        Compute embedding for a document and persist to Pinecone.

        Args:
            text: Document content to embed
            doc_id: Document ID (for Pinecone ID)
            metadata: Document metadata for Pinecone filter

        Returns:
            Pinecone vector ID
        """
        try:
            # Compute embedding via Cohere API
            embedding = self._get_embedding(text)
            if not embedding:
                logger.warning(f"Failed to embed document {doc_id}")
                return ""

            # Upsert to Pinecone with metadata
            vector_id = f"doc_{doc_id}"
            self.pinecone.upsert_embeddings(
                [(vector_id, embedding, metadata)]
            )

            return vector_id

        except Exception as e:
            logger.error(f"Failed to process embedding for {doc_id}: {e}")
            return ""

    def embed_batch(self, texts: List[str], doc_ids: List[str], metadatas: List[dict]) -> List[str]:
        """
        Embed multiple documents in batch.

        Args:
            texts: List of document contents
            doc_ids: List of document IDs
            metadatas: List of metadata dicts

        Returns:
            List of vector IDs
        """
        try:
            embeddings = self._get_embeddings_batch(texts)
            if not embeddings:
                logger.warning(f"Failed to embed batch of {len(texts)} documents")
                return []

            # Prepare vectors for Pinecone
            vectors = []
            vector_ids = []
            for doc_id, embedding, metadata in zip(doc_ids, embeddings, metadatas):
                vector_id = f"doc_{doc_id}"
                vectors.append((vector_id, embedding, metadata))
                vector_ids.append(vector_id)

            # Upsert to Pinecone
            if vectors:
                self.pinecone.upsert_embeddings(vectors)
                logger.info(f"Embedded and persisted {len(vectors)} documents")

            return vector_ids

        except Exception as e:
            logger.error(f"Failed to process embedding batch: {e}")
            return []

    def _get_embedding(self, text: str) -> list:
        """Get embedding from Cohere API."""
        if not self.api_key:
            logger.warning("Cohere API key not configured; using zero vector")
            return [0.0] * 1536  # Placeholder

        try:
            response = self.client.post(
                "https://api.cohere.ai/v1/embed",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model,
                    "texts": [text],
                    "input_type": "search_document",
                },
            )
            response.raise_for_status()
            data = response.json()
            embeddings = data.get("embeddings", [])
            return embeddings[0] if embeddings else []
        except httpx.RequestError as e:
            logger.error(f"Failed to get embedding from Cohere: {e}")
            return []

    def _get_embeddings_batch(self, texts: List[str]) -> List[list]:
        """Get embeddings for multiple texts from Cohere API."""
        if not self.api_key:
            logger.warning("Cohere API key not configured; using zero vectors")
            return [[0.0] * 1536 for _ in texts]

        try:
            response = self.client.post(
                "https://api.cohere.ai/v1/embed",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model,
                    "texts": texts,
                    "input_type": "search_document",
                },
            )
            response.raise_for_status()
            data = response.json()
            return data.get("embeddings", [])
        except httpx.RequestError as e:
            logger.error(f"Failed to get embeddings from Cohere: {e}")
            return []

    def close(self):
        """Close connections."""
        self.client.close()
        self.pinecone.close()
