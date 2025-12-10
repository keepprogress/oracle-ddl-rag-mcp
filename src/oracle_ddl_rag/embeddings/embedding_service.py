"""具有自動偵測功能的語意搜尋嵌入服務。"""

import os
from abc import ABC, abstractmethod
from typing import Optional

from ..config import (
    OPENAI_EMBEDDING_MODEL,
    OPENAI_EMBEDDING_DIMS,
    LOCAL_EMBEDDING_MODEL,
)


class EmbeddingService(ABC):
    """嵌入服務的抽象基底類別。"""

    @abstractmethod
    def embed(self, texts: list[str]) -> list[list[float]]:
        """為文字列表產生嵌入向量。

        參數：
            texts: 要嵌入的文字字串列表。

        回傳：
            嵌入向量列表（每個輸入文字一個）。
        """
        pass

    @abstractmethod
    def embed_single(self, text: str) -> list[float]:
        """為單一文字產生嵌入向量。

        參數：
            text: 要嵌入的文字字串。

        回傳：
            嵌入向量。
        """
        pass

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """回傳嵌入的維度數。"""
        pass

    @property
    @abstractmethod
    def model_name(self) -> str:
        """回傳使用的模型名稱。"""
        pass


class OpenAIEmbedding(EmbeddingService):
    """使用 text-embedding-3-small 的 OpenAI 嵌入服務。"""

    def __init__(
        self,
        model: str = OPENAI_EMBEDDING_MODEL,
        dims: int = OPENAI_EMBEDDING_DIMS,
    ):
        """初始化 OpenAI 嵌入客戶端。

        參數：
            model: OpenAI 模型名稱。
            dims: 嵌入維度數。
        """
        from openai import OpenAI

        self._client = OpenAI()  # 使用 OPENAI_API_KEY 環境變數
        self._model = model
        self._dims = dims

    def embed(self, texts: list[str]) -> list[list[float]]:
        """使用 OpenAI API 產生嵌入向量。"""
        if not texts:
            return []

        response = self._client.embeddings.create(
            input=texts,
            model=self._model,
            dimensions=self._dims,
        )
        return [item.embedding for item in response.data]

    def embed_single(self, text: str) -> list[float]:
        """為單一文字產生嵌入向量。"""
        return self.embed([text])[0]

    @property
    def dimensions(self) -> int:
        return self._dims

    @property
    def model_name(self) -> str:
        return self._model


class LocalEmbedding(EmbeddingService):
    """使用 sentence-transformers 的本地嵌入服務。"""

    def __init__(self, model_name: str = LOCAL_EMBEDDING_MODEL):
        """初始化本地嵌入模型。

        參數：
            model_name: HuggingFace 模型名稱或路徑。
        """
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(model_name)
        self._model_name = model_name

    def embed(self, texts: list[str]) -> list[list[float]]:
        """使用本地模型產生嵌入向量。"""
        if not texts:
            return []

        embeddings = self._model.encode(texts, convert_to_numpy=True)
        return embeddings.tolist()

    def embed_single(self, text: str) -> list[float]:
        """為單一文字產生嵌入向量。"""
        return self.embed([text])[0]

    @property
    def dimensions(self) -> int:
        return self._model.get_sentence_embedding_dimension()

    @property
    def model_name(self) -> str:
        return self._model_name


# 單例實例
_embedding_service: Optional[EmbeddingService] = None


def get_embedding_service(force_local: bool = False) -> EmbeddingService:
    """取得嵌入服務實例（自動偵測或快取）。

    自動偵測邏輯：
    1. 若設定了 OPENAI_API_KEY，使用 OpenAI 嵌入
    2. 否則，使用本地 sentence-transformers 模型

    參數：
        force_local: 若為 True，無論 API 金鑰都使用本地模型。

    回傳：
        EmbeddingService 實例。
    """
    global _embedding_service

    # 若有快取且非強制本地，回傳快取實例
    if _embedding_service is not None and not force_local:
        return _embedding_service

    # 根據環境自動偵測
    if not force_local and os.environ.get("OPENAI_API_KEY"):
        _embedding_service = OpenAIEmbedding()
    else:
        _embedding_service = LocalEmbedding()

    return _embedding_service


def reset_embedding_service() -> None:
    """重置快取的嵌入服務（用於測試）。"""
    global _embedding_service
    _embedding_service = None
