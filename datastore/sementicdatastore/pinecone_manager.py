from pinecone import Pinecone
import logging

class PineconeDatastore:
    def __init__(self, api_key, index_name):
        self.pc = Pinecone(api_key=api_key)
        self.index = self.pc.Index(index_name)

    def upsert_vectors(self, vectors):
        """
        Writes vectors to Pinecone.
        Expects a list of dicts: [{'id': str, 'values': list, 'metadata': dict}]
        """
        try:
            self.index.upsert(vectors=vectors)
            return True
        except Exception as e:
            logging.error(f"Pinecone Upsert Error: {e}")
            return False

    def delete_vectors(self, ids):
        """Removes specific vectors by ID."""
        if not ids:
            return
        try:
            self.index.delete(ids=ids)
        except Exception as e:
            logging.error(f"Pinecone Delete Error: {e}")

    def query_semantic(self, vector, top_k=5, filter_dict=None):
        """Reads/Searches vectors from Pinecone."""
        return self.index.query(
            vector=vector,
            top_k=top_k,
            include_metadata=True,
            filter=filter_dict
        )
