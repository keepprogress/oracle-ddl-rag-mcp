"""Embedding services for semantic search with auto-detect capability."""

import os
from abc import ABC, abstractmethod
from typing import Optional

from ..config import (
    OPENAI_EMBEDDING_MODEL,
    OPENAI_EMBEDDING_DIMS,
    LOCAL_EMBEDDING_MODEL,
)


class EmbeddingService(ABC):
    """Abstract base class for embedding services."""

    @abstractmethod
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for a list of texts.

        Args:
            texts: List of text strings to embed.

        Returns:
            List of embedding vectors (one per input text).
        """
        pass

    @abstractmethod
    def embed_single(self, text: str) -> list[float]:
        """Generate embedding for a single text.

        Args:
            text: Text string to embed.

        Returns:
            Embedding vector.
        """
        pass

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """Return the dimensionality of embeddings."""
        pass

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Return the model name being used."""
        pass


class OpenAIEmbedding(EmbeddingService):
    """OpenAI embedding service using text-embedding-3-small."""

    def __init__(
        self,
        model: str = OPENAI_EMBEDDING_MODEL,
        dims: int = OPENAI_EMBEDDING_DIMS,
    ):
        """Initialize OpenAI embedding client.

        Args:
            model: OpenAI model name.
            dims: Number of embedding dimensions.
        """
        from openai import OpenAI

        self._client = OpenAI()  # Uses OPENAI_API_KEY env var
        self._model = model
        self._dims = dims

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings using OpenAI API."""
        if not texts:
            return []

        response = self._client.embeddings.create(
            input=texts,
            model=self._model,
            dimensions=self._dims,
        )
        return [item.embedding for item in response.data]

    def embed_single(self, text: str) -> list[float]:
        """Generate embedding for single text."""
        return self.embed([text])[0]

    @property
    def dimensions(self) -> int:
        return self._dims

    @property
    def model_name(self) -> str:
        return self._model


class LocalEmbedding(EmbeddingService):
    """Local embedding service using sentence-transformers."""

    def __init__(self, model_name: str = LOCAL_EMBEDDING_MODEL):
        """Initialize local embedding model.

        Args:
            model_name: HuggingFace model name or path.
        """
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(model_name)
        self._model_name = model_name

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings using local model."""
        if not texts:
            return []

        embeddings = self._model.encode(texts, convert_to_numpy=True)
        return embeddings.tolist()

    def embed_single(self, text: str) -> list[float]:
        """Generate embedding for single text."""
        return self.embed([text])[0]

    @property
    def dimensions(self) -> int:
        return self._model.get_sentence_embedding_dimension()

    @property
    def model_name(self) -> str:
        return self._model_name


# Singleton instance
_embedding_service: Optional[EmbeddingService] = None


def get_embedding_service(force_local: bool = False) -> EmbeddingService:
    """Get the embedding service instance (auto-detect or cached).

    Auto-detection logic:
    1. If OPENAI_API_KEY is set, use OpenAI embeddings
    2. Otherwise, use local sentence-transformers model

    Args:
        force_local: If True, always use local model regardless of API key.

    Returns:
        EmbeddingService instance.
    """
    global _embedding_service

    # Return cached instance if available and not forcing local
    if _embedding_service is not None and not force_local:
        return _embedding_service

    # Auto-detect based on environment
    if not force_local and os.environ.get("OPENAI_API_KEY"):
        _embedding_service = OpenAIEmbedding()
    else:
        _embedding_service = LocalEmbedding()

    return _embedding_service


def reset_embedding_service() -> None:
    """Reset the cached embedding service (useful for testing)."""
    global _embedding_service
    _embedding_service = None
