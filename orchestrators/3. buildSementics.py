# This flow embedd data and persist in pinecone.
import logging
import os
import sys
import configparser
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Dynamic Path Resolution to find your 'datastore' and 'service' packages
current_file_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file_path)))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datastore.rawdatastore.postgresManager import PostgresDatastore
from datastore.sementicdatastore.pinecone_manager import PineconeDatastore
from service.embedding_service import EmbeddingService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VectorSyncWorker:
    def __init__(self, config):
        self.db = PostgresDatastore({
            "dbname": config.get('database', 'dbname'),
            "user": config.get('database', 'user'),
            "password": config.get('database', 'password'),
            "host": config.get('database', 'host'),
            "port": config.getint('database', 'port')
        })
        self.embeddings = EmbeddingService()
        self.pinecone = PineconeDatastore(
            api_key=config.get('pinecone', 'api_key'),
            index_name=config.get('pinecone', 'index_name')
        )



    def _process_single_row(self, row):
        """
        Processes a single row. This function will be executed in a separate thread.
        """
        m_id, m_name, c_name, ret_type, summary, meta_json, src_path = row
        
        params = meta_json.get('parameters', '()') if meta_json else '()'
        details = meta_json.get('details', '') if meta_json else ''
        signature = f'{ret_type} {m_name}{params}'
        
        searchable_text = (
            f"Class: {c_name} | Method: {m_name}\n"
            f"Signature: {signature}\n"
            f"Summary: {summary}"
        )
        print(f'About to process for class : {c_name}.{signature}')
        try:
            # This is the slow network call we are parallelizing
            vector, tokens = self.embeddings.get_vector_with_usage(searchable_text)

            metadata = {
                "method_name": m_name,
                "class_name": c_name,
                "source_path": src_path or "unknown",
                "ai_summary": summary,
                "ai_details": details,
                "signature": signature
            }

            upsert_data = {
                "id": str(m_id),
                "values": vector,
                "metadata": metadata
            }
            
            # (id, tokens, cost)
            usage_data = (m_id, tokens, (tokens / 1000) * 0.00002)
            
            return upsert_data, usage_data, None # No error

        except Exception as e:
            logging.error(f"❌ Failed to vectorize method {m_id}: {e}")
            # Return the ID and the error to handle it later
            return None, None, m_id

    def _sync_rows(self, rows, max_workers=10):
        """
        Uses a ThreadPool to process all rows in parallel.
        """
        vectors_to_upsert = []
        usage_tracking = []
        failed_ids = []

        # 
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Map the processing function to each row
            future_to_row = {executor.submit(self._process_single_row, row): row for row in rows}

            for future in as_completed(future_to_row):
                upsert_data, usage_data, error_id = future.result()
                
                if error_id:
                    failed_ids.append(error_id)
                else:
                    vectors_to_upsert.append(upsert_data)
                    usage_tracking.append(usage_data)

        # Handle failed IDs by updating DB status (could also be bulk updated)
     

        return vectors_to_upsert, usage_tracking

    def sync(self, batch_size=100):
        logging.info("Checking for methods to vectorize...")
        rows = self.db.get_methods_to_vectorize(limit=batch_size)
        
        if not rows:
            logging.info("✨ Everything is up to date.")
            return 0
        vectors_to_upsert, usage_tracking = self._sync_rows(rows)
        
        # 2. Bulk Upsert to Pinecone
        if vectors_to_upsert:
            if self.pinecone.upsert_vectors(vectors_to_upsert):
                logging.info(f"✅ Upserted {len(vectors_to_upsert)} vectors. Updating Postgres usage...")
                
                self.db.bulk_update_vector_sync(usage_tracking)
                
                logging.info("✨ Postgres vector_tokens and cost updated.")
        return len(rows)
                
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config_path = os.path.join(project_root, 'config.ini')
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found at {config_path}")
        sys.exit(1)
        
    config.read(config_path)
    worker = VectorSyncWorker(config)
    
    itemsProcessed = worker.sync()
    while itemsProcessed > 0 :
        itemsProcessed = worker.sync()
        time.sleep(1)
