import logging
import psycopg2
import json
from psycopg2.extras import execute_values
from datetime import datetime

class PostgresDatastore:
    def __init__(self, config):
        self.conn = psycopg2.connect(**config)

    def upsert_methods(self, methods):
        """
        Inserts or updates methods from the Java Parser.
        Now resets status to 'new' or 'updated' and vector_status to 'pending' on change.
        """
        current_now = datetime.now()
        query = """
        INSERT INTO java_methods (
            id, source_path, component, package_name, class_name, 
            method_name, visibility, return_type, method_code, javadoc_raw,
            internal_deps, external_libs, content_hash, line_count,
            status, vector_status, last_seen_at
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            status = CASE 
                WHEN java_methods.content_hash != EXCLUDED.content_hash THEN 'updated'
                ELSE java_methods.status 
            END,
            vector_status = CASE 
                WHEN java_methods.content_hash != EXCLUDED.content_hash THEN 'pending'
                ELSE java_methods.vector_status 
            END,
            method_code = EXCLUDED.method_code,
            javadoc_raw = EXCLUDED.javadoc_raw,
            internal_deps = EXCLUDED.internal_deps,
            external_libs = EXCLUDED.external_libs,
            content_hash = EXCLUDED.content_hash,
            line_count = EXCLUDED.line_count,
            last_seen_at = EXCLUDED.last_seen_at;
        """
    
        data = [(
            m['id'], m['source'], m['component'], m['package'], m['class_name'], 
            m['method_name'], m['visibility'], m['return_type'], m['method_code'], 
            m['javadoc_raw'], m['internal_deps'], m['external_libs'],
            m['content_hash'], m['line_count'],
            'new', 'pending', current_now
        ) for m in methods]
        
        with self.conn.cursor() as cur:
            execute_values(cur, query, data)
            self.conn.commit()

    def update_ai_enrichment(self, method_id, summary, details, is_accurate, metadata_dict, tokens=0, cost=0.0):
        """
        Updates the record with AI analysis results, JSON metadata, and cost tracking.
        'metadata_dict' should contain use_cases, logic_intent, etc.
        """
        query = """
            UPDATE java_methods 
            SET ai_summary = %s, 
                ai_details = %s, 
                is_doc_accurate = %s, 
                metadata_json = %s,
                summary_tokens = %s,
                total_cost = total_cost + %s,
                status = 'ai_processed', 
                processed_at = %s 
            WHERE id = %s;
        """
        # Convert dict to JSON string for Postgres JSONB
        json_metadata = json.dumps(metadata_dict)
        
        with self.conn.cursor() as cur:
            cur.execute(query, (
                summary, details, is_accurate, json_metadata, 
                tokens, cost, datetime.now(), method_id
            ))
            self.conn.commit()

    def update_vector_sync_status(self, method_id, tokens=0, cost=0.0):
        """
        Updates cost after a successful Pinecone upsert.
        """
        query = """
            UPDATE java_methods 
            SET vector_status = 'synced',
                vector_tokens = %s,
                total_cost = total_cost + %s
            WHERE id = %s;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (tokens, cost, method_id))
            self.conn.commit()

    def update_vector_sync_status_batch(self, method_ids, status='synced'):
        """
        Updates status for multiple methods at once.
        Note: Cost/Token tracking is usually handled per-batch for efficiency.
        """
        if not method_ids:
            return
            
        query = "UPDATE java_methods SET vector_status = %s WHERE id = ANY(%s);"
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (status, method_ids))
                self.conn.commit()
                logging.info(f"Updated status to '{status}' for {len(method_ids)} records.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Postgres Update Error: {e}")
        
    def get_methods_to_analyze(self, limit=50):
        """Fetches methods that need LLM enrichment."""
        query = """
            SELECT id, method_name, class_name,visibility, return_type, method_code, javadoc_raw 
            FROM java_methods 
            WHERE status IN ('new', 'updated') 
            ORDER BY line_count DESC
            LIMIT %s;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (limit,))
            return cur.fetchall()

    def get_methods_to_vectorize(self, limit=100):
        """Fetches AI-processed methods that aren't in Pinecone yet."""
        query = """
            SELECT id, method_name, class_name, return_type, ai_summary, metadata_json, source_path
            FROM java_methods 
            WHERE vector_status = 'pending' AND status = 'ai_processed' AND NOT (metadata_json @> '{"is_getter_setter": true}')
            LIMIT %s;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (limit,))
            return cur.fetchall()

    def bulk_update_vector_sync(self, update_data):
        """
        update_data: List of tuples [(id, tokens, cost), ...]
        """
        if not update_data:
            return

        query = """
            UPDATE java_methods AS m
            SET 
                vector_tokens = v.tokens,
                total_cost = total_cost + v.cost,
                vector_status = 'synced'
            FROM (VALUES %s) AS v(id, tokens, cost)
            WHERE m.id = v.id
        """
        
        try:
            with self.conn.cursor() as cur:
                # execute_values is a specialized psycopg2 helper for bulk operations
                from psycopg2.extras import execute_values
                execute_values(cur, query, update_data)
                self.conn.commit()
                logging.info(f"Successfully bulk updated {len(update_data)} rows.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Bulk update failed: {e}")

    def get_stale_methods(self, repo_path_prefix, scan_start_time):
        query = """
            SELECT id FROM java_methods 
            WHERE source_path LIKE %s 
            AND last_seen_at < %s;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (f"{repo_path_prefix}%", scan_start_time))
            return [row[0] for row in cur.fetchall()]

    def delete_methods_by_id(self, method_ids):
        if not method_ids: return
        query = "DELETE FROM java_methods WHERE id = ANY(%s);"
        with self.conn.cursor() as cur:
            cur.execute(query, (method_ids,))
            self.conn.commit()

    def close(self):
        self.conn.close()
