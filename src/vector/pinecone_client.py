import logging
from typing import List, Dict, Any, Optional
from pinecone import Pinecone, ServerlessSpec
from src.config import settings

logger = logging.getLogger(__name__)


class PineconeClient:
    """Wrapper around Pinecone for vector operations."""

    def __init__(self):
        try:
            self.client = Pinecone(api_key=settings.pinecone_api_key)
            # Index is assumed to already exist; if not, uncomment to create:
            # self._ensure_index()
            self.index = self.client.Index(settings.pinecone_index_name)
        except Exception as e:
            logger.error(f"Failed to initialize Pinecone: {e}")
            self.client = None
            self.index = None

    def _ensure_index(self):
        """Create index if it doesn't exist."""
        index_name = settings.pinecone_index_name
        if index_name not in self.client.list_indexes().names():
            logger.info(f"Creating Pinecone index: {index_name}")
            self.client.create_index(
                name=index_name,
                dimension=1536,  # OpenAI/Cohere embedding dimension
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )

    def upsert_embeddings(
        self,
        vectors: List[tuple[str, List[float], Dict[str, Any]]],
    ):
        """
        Upsert vectors to Pinecone.

        Args:
            vectors: List of (id, embedding, metadata) tuples
        """
        if not self.index:
            logger.warning("Pinecone not initialized; skipping upsert")
            return

        try:
            self.index.upsert(vectors=vectors)
            logger.info(f"Upserted {len(vectors)} vectors to Pinecone")
        except Exception as e:
            logger.error(f"Failed to upsert vectors: {e}")

    def query(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filter_dict: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query Pinecone for similar vectors.

        Args:
            query_vector: Embedding to search for
            top_k: Number of results to return
            filter_dict: Optional metadata filter

        Returns:
            List of matching results with scores
        """
        if not self.index:
            logger.warning("Pinecone not initialized; returning empty results")
            return []

        try:
            results = self.index.query(
                vector=query_vector,
                top_k=top_k,
                include_metadata=True,
                filter=filter_dict,
            )
            return results.get("matches", [])
        except Exception as e:
            logger.error(f"Failed to query Pinecone: {e}")
            return []

    def delete_by_metadata(self, filter_dict: Dict[str, Any]):
        """Delete vectors matching metadata filter."""
        if not self.index:
            logger.warning("Pinecone not initialized; skipping delete")
            return

        try:
            self.index.delete(filter=filter_dict)
            logger.info(f"Deleted vectors matching filter: {filter_dict}")
        except Exception as e:
            logger.error(f"Failed to delete vectors: {e}")

    def close(self):
        """Close connection."""
        pass  # Pinecone client is stateless
