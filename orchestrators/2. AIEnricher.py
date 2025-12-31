# This enrich the methods scanned in import project functionality
import sys
import os
import logging
import re
import configparser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# 1. Dynamic Path Resolution
current_dir = os.path.dirname(os.path.abspath(__file__))
# Assumes config.ini is in the parent directory (project root)
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from datastore.rawdatastore.postgresManager import PostgresDatastore
from summarizer.analyzer import CodeAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AIEnricher:
    def __init__(self, db: PostgresDatastore, analyzer: CodeAnalyzer):
        self.db = db
        self.analyzer = analyzer

    def is_getter_setter(self, method_name, code):
        """Heuristic to detect simple Java getters and setters."""
        
        # 1. Remove Multi-line comments: /* ... */ (DOTALL is needed here)
        clean_code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        
        # 2. Remove Single-line comments: // ... (NO DOTALL here, stop at newline)
        clean_code = re.sub(r'//.*', '', clean_code).strip()
        
        line_count = len(clean_code.splitlines())
        
        is_standard_name = bool(re.match(r'^(get|set|is)[A-Z]', method_name))
        
        # Add the complexity check we discussed to protect reflection/logic
        logic_keywords = ['if', 'for', 'while', 'invoke', 'try', 'stream']
        has_logic = any(keyword in clean_code for keyword in logic_keywords)

        is_boilerplate = is_standard_name and line_count <= 4 and not has_logic
        
        return is_boilerplate
    
    def generate_local_analysis(self, method_name, class_name):
        """Generates analysis for boilerplate locally (0 cost)."""
        field_name = method_name[3:] if method_name.startswith(('get', 'set')) else method_name[2:]
        if method_name.startswith('set'):
            summary = f"Standard setter for '{field_name}'."
            role = "Updates internal object state."
        else:
            summary = f"Standard getter for '{field_name}'."
            role = "Exposes internal object state."

        return {
            "summary": summary,
            "logic_intent": "Standard field access.",
            "internal_role": role,
            "use_cases": ["Data access/modification"],
            "details": "Standard boilerplate.",
            "doc_match": "Yes",
            "name_match": True
        }

    def process_batches(self, batch_size=20):
        """Enriches new or updated methods from the DB."""
        logging.info(f"🔍 Fetching {batch_size} methods...")
        methods = self.db.get_methods_to_analyze(limit=batch_size)
        if not methods:
            logging.info("✨ No pending methods found.")
            return 0
        self._process_list(methods)
        return len(methods)

    def process_specific_methods(self, method_ids: list):
        """Enriches specific methods by ID."""
        logging.info(f"🎯 Targeting {len(method_ids)} IDs...")
        query = """
            SELECT id, method_name, class_name, visibility, return_type, method_code, javadoc_raw 
            FROM java_methods WHERE id = ANY(%s)
        """
        with self.db.conn.cursor() as cur:
            cur.execute(query, (method_ids,))
            self._process_list(cur.fetchall())

    def _process_list(self, methods):
        """Uses a ThreadPool to process methods in parallel."""
        # Adjust max_workers based on your API rate limits (e.g., 5 to 10)
        max_workers = 10 
        
        logging.info(f"🚀 Starting ThreadPool with {max_workers} workers")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            #executor.map(self._process_method, methods)
            futures = [executor.submit(self._process_method, m) for m in methods]
            
            for future in futures:
                try:
                    # This will re-raise any exception that happened inside the thread
                    future.result() 
                except Exception as e:
                    logging.error(f"Thread generated an exception: {e}")
        
    def _process_method(self, method) :    
        m_id, m_name, c_name, v_type, ret_type, code, javadoc = method
        logging.info(f"🤖 Analyzing: {c_name}.{m_name} ({v_type})")
        
        try :
            if self.is_getter_setter(m_name, code):
                analysis = self.generate_local_analysis(m_name, c_name)
                tokens, cost = 0, 0.0
            else:
                analysis = self.analyzer.get_analysis(m_name, c_name, code, v_type, javadoc)
                tokens = analysis.get('_tokens', 0) if analysis else 0
                cost = analysis.get('_cost', 0.0) if analysis else 0.0

            if analysis:
                meta = {
                    "logic_intent": analysis.get('logic_intent', ""),
                    "is_getter_setter": self.is_getter_setter(m_name, code),
                    "return_type": ret_type,
                    "visibility": v_type
                }
                
                # Split based on visibility logic
                if v_type.lower() == "public":
                    meta["use_cases"] = analysis.get('use_cases', [])
                else:
                    meta["internal_role"] = analysis.get('internal_role', "")

            self.db.update_ai_enrichment(
                m_id, analysis['summary'], analysis['details'], 
                analysis.get('doc_match') == 'Yes', meta, tokens, cost
            )
        except Exception as e:
            logging.error(f"❌ Error processing {m_name}: {e}")

if __name__ == "__main__":
    # 1. Find config.ini in project root
    config = configparser.ConfigParser()
    config_file = os.path.join(project_root, 'config.ini')
    
    if not os.path.exists(config_file):
        logging.error(f"❌ Config file not found at {config_file}")
        sys.exit(1)
        
    config.read(config_file)

    # 2. Extract DB Params
    db_params = {
        "dbname": config.get('database', 'dbname'),
        "user": config.get('database', 'user'),
        "password": config.get('database', 'password'),
        "host": config.get('database', 'host'),
        "port": config.getint('database', 'port')
    }

    # 3. Initialize and Run
    db_store = PostgresDatastore(db_params)
    analyzer = CodeAnalyzer()
    enricher = AIEnricher(db_store, analyzer)
    
   
    methods = enricher.process_batches(batch_size=100)
    while methods > 0 :
        methods = enricher.process_batches(batch_size=100)
    db_store.close()
