#this flow scans all java files, extract methods, their details and persist it to DB
import os
import logging
from extractor.parser import JavaExtractor
from datastore.rawdatastore.postgresManager import PostgresDatastore
import configparser

# Initialize the parser
config = configparser.ConfigParser()
config.read('config.ini')

# --- 1. CONFIGURATION ---
# Access the values as a dictionary
DB_CONFIG = {
    "host": config['database']['host'],
    "port": config['database']['port'],
    "dbname": config['database']['dbname'],
    "user": config['database']['user'],
    "password": config['database']['password']
}

SOURCE_DIR = config['BaseConfig']['java_source']
BATCH_SIZE = 50

# --- 2. LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_extraction_pipeline():
    # Initialize our components
    extractor = JavaExtractor()
    db = PostgresDatastore(DB_CONFIG)
    
    try:
        logger.info(f"🚀 Starting extraction from: {SOURCE_DIR}")
        
        # Step 1: Scan files and extract all methods
        # Assuming your extractor returns a list of dicts
        all_methods = extractor.extract_from_directory(SOURCE_DIR)
        total_found = len(all_methods)
        logger.info(f"✅ Found {total_found} methods. Starting DB Upsert...")

        # Step 2: Batch Upsert to Postgres
        for i in range(0, total_found, BATCH_SIZE):
            batch = all_methods[i : i + BATCH_SIZE]
            db.upsert_methods(batch)
            logger.info(f"📦 Batched {i + len(batch)} / {total_found} methods into DB.")

        logger.info("🏁 Pipeline finished successfully!")

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
    
    finally:
        db.close()
        logger.info("🔌 Database connection closed.")

if __name__ == "__main__":
    run_extraction_pipeline()
