import os
from openai import OpenAI

class EmbeddingService:
    def __init__(self, model="text-embedding-3-small", dimensions=1536):
        self.client = OpenAI()
        self.model = model
        self.dimensions = dimensions

    def get_vector_with_usage(self, text):
        """Returns (embedding_vector, token_usage_count)"""
        text = text.replace("\n", " ").strip()
        
        if len(text) > 30000:
            text = text[:30000]

        response = self.client.embeddings.create(
            input=[text],
            model=self.model,
            dimensions=self.dimensions
        )
        
        vector = response.data[0].embedding
        usage = response.usage.total_tokens  # This is the actual token count
        return vector, usage
