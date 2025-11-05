import pandas as pd
import os
import time
import logging
from sqlalchemy import create_engine

# --- Configuration ---

# 1. Logging Setup (FIXED: ensures 'logs' directory exists)
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "ingestion_db.log")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# 2. Database Credentials (UPDATE these if different)
# Format: mysql+mysqlconnector://user:password@host/database_name
db_url = "mysql+mysqlconnector://root:password@localhost/coffee"
engine = create_engine(db_url)

# 3. Data File Location
DATA_DIR = 'data'
FILE_NAME = 'sales.csv'
FILE_PATH = os.path.join(DATA_DIR, FILE_NAME) 

# --- Database Ingestion Function ---

# This function will ingest the dataframe into database table
def inject_db(df, table_name, engine):
    """Ingests a DataFrame into a specified database table."""
    try:
        print(f"Injecting data into table: {table_name}...") 
        
        # FIX: Pass the 'engine' object directly to 'con' for proper transaction management
        df.to_sql(table_name, 
                  con=engine, 
                  if_exists='replace', 
                  index=False,
                  chunksize=1000 # Good practice for large files
                  )
        logging.info(f"✅ Successfully created/replaced table: {table_name}")
        print(f"✅ Successfully created/replaced table: {table_name}")
    except Exception as e:
        logging.error(f"❌ Error with table {table_name}.", exc_info=True)
        print(f"❌ Error with table {table_name}. Transaction rolled back.")
        print(f"   Reason: {e}")


# --- Data Loading and Execution ---

def load_raw_data():
    """Loads a single CSV file and ingests it into the database."""
    start = time.time()
    
    if os.path.exists(FILE_PATH):
        try:
            # Table name will be 'Coffe_sales' (the filename without .csv)
            table_name = FILE_NAME.replace('.csv', '') 
            
            df = pd.read_csv(FILE_PATH)
            logging.info(f'Loaded {FILE_NAME}. Ingesting into db as table "{table_name}"')
            
            inject_db(df, table_name, engine)
            
        except Exception as e:
            logging.error(f'Failed to load or ingest {FILE_NAME}.', exc_info=True)
            print(f"A critical error occurred: {e}")
            
    else:
        logging.warning(f'Data file not found at: {FILE_PATH}')
        print(f"🛑 Error: Data file not found at: {FILE_PATH}")

    end = time.time()
    total_time = (end - start) / 60
    
    logging.info('Ingestion Complete')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')
    print(f"\n--- Script Finished ---")
    print(f"Total Time Taken: {total_time:.2f} minutes")


if __name__ == '__main__':
    load_raw_data()