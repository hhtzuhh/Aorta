"""
Gemini Embedding Service

Generates embeddings using Google's Gemini API
for vector search in MongoDB Atlas.
"""

import asyncio
from typing import List
from google import genai
from google.genai import types


class GeminiEmbeddingService:
    """Generate embeddings using Gemini API"""

    def __init__(self, api_key: str, model: str = "gemini-embedding-001"):
        """
        Initialize Gemini embedding service

        Args:
            api_key: Google Gemini API key
            model: Embedding model to use (gemini-embedding-001, output 768 dimensions)
        """
        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.embedding_dimension = 768

    async def embed_text(self, text: str) -> List[float]:
        """
        Generate embedding for a single text

        Args:
            text: Text to embed

        Returns:
            Embedding vector (768 dimensions)
        """
        response = await self.client.aio.models.embed_content(
            model=self.model,
            contents=text,
            config=types.EmbedContentConfig(
                task_type="RETRIEVAL_DOCUMENT",
                output_dimensionality=768
            )
        )
        # Response has 'embeddings' list, get first one's 'values'
        return response.embeddings[0].values

    async def embed_batch(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
        """
        Generate embeddings for multiple texts

        Args:
            texts: List of texts to embed
            batch_size: Number of texts per batch

        Returns:
            List of embedding vectors
        """
        embeddings = []

        # Process in batches
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1} ({len(batch)} texts)")

            # Generate embeddings for batch
            batch_embeddings = await asyncio.gather(*[
                self.embed_text(text) for text in batch
            ])

            embeddings.extend(batch_embeddings)

            # Rate limiting
            await asyncio.sleep(0.5)

        return embeddings

    def embed_text_sync(self, text: str) -> List[float]:
        """
        Synchronous version of embed_text

        Args:
            text: Text to embed

        Returns:
            Embedding vector
        """
        return asyncio.run(self.embed_text(text))

    def embed_batch_sync(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
        """
        Synchronous version of embed_batch

        Args:
            texts: List of texts to embed
            batch_size: Number of texts per batch

        Returns:
            List of embedding vectors
        """
        return asyncio.run(self.embed_batch(texts, batch_size))
