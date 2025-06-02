import os
import glob
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
from datetime import datetime
from decimal import Decimal, InvalidOperation
from bson.decimal128 import Decimal128
import logging
from dotenv import load_dotenv
import time # Import time for sleep

# --- Configuration & Setup ---
# ... (rest of the setup remains the same)
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
MONGO_USER = os.getenv('MONGO_INITDB_ROOT_USERNAME')
MONGO_PASS = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
MONGO_COLLECTION_NAME = os.getenv('MONGO_COLLECTION_NAME')
DATA_DIR = "/usr/src/app/data"
PROCESSED_FILES_LOG = "/usr/src/app/data/processed_files.log"

# --- Helper Functions ---

def get_mongo_client(max_retries=5, retry_delay=10): # Added retry parameters
    """Establishes a connection to MongoDB with retries."""
    mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
    attempt = 0
    while attempt < max_retries:
        try:
            logging.info(f"Attempting to connect to MongoDB (attempt {attempt + 1}/{max_retries})...")
            # Add serverSelectionTimeoutMS to MongoClient to control how long to wait for server selection
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) # 5 second timeout for server selection
            # The ismaster command is cheap and does not require auth.
            client.admin.command('ismaster') # Check connection
            logging.info(f"Successfully connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
            return client
        except (ConnectionFailure, ServerSelectionTimeoutError) as e: # Catch ServerSelectionTimeoutError as well
            attempt += 1
            logging.warning(f"MongoDB connection failed (attempt {attempt}/{max_retries}): {e}. Retrying in {retry_delay} seconds...")
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to connect to MongoDB after {max_retries} attempts.")
                return None
        except Exception as e: # Catch any other unexpected errors during connection
            attempt +=1
            logging.error(f"An unexpected error occurred while trying to connect to MongoDB: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to connect to MongoDB after {max_retries} attempts due to unexpected error.")
                return None
    return None


# ... (rest of the helper functions: clean_name, parse_date, to_decimal128, get_processed_files, mark_file_as_processed remain the same) ...
def clean_name(name_str):
    """Cleans and standardizes a name string."""
    if pd.isna(name_str):
        return None
    return str(name_str).strip().title()

def parse_date(date_str, date_format="%d/%m/%Y"):
    """Parses a date string into a datetime object."""
    if pd.isna(date_str) or date_str == "":
        return None
    try:
        return datetime.strptime(str(date_str).strip(), date_format)
    except ValueError:
        logging.warning(f"Could not parse date: {date_str} with format {date_format}. Returning None.")
        return None

def to_decimal128(value_str):
    """Converts a string to Decimal128 for MongoDB, handling potential errors."""
    if pd.isna(value_str):
        return None
    try:
        py_decimal = Decimal(str(value_str).strip())
        return Decimal128(py_decimal)
    except InvalidOperation:
        logging.warning(f"Could not convert billing amount '{value_str}' to Decimal. Returning None.")
        return None
    except Exception as e:
        logging.warning(f"Error converting '{value_str}' to Decimal128: {e}. Returning None.")
        return None

def get_processed_files():
    """Reads the list of already processed files."""
    if not os.path.exists(PROCESSED_FILES_LOG):
        return set()
    with open(PROCESSED_FILES_LOG, 'r') as f:
        return set(line.strip() for line in f)

def mark_file_as_processed(filename):
    """Adds a filename to the list of processed files."""
    with open(PROCESSED_FILES_LOG, 'a') as f:
        f.write(filename + '\n')

def process_csv_file(filepath, db_collection, source_filename):
    """Loads, cleans, validates, and inserts data from a single CSV file."""
    logging.info(f"Processing file: {filepath}")
    try:
        df = pd.read_csv(filepath, delimiter=';')
        logging.info(f"Successfully loaded {len(df)} rows from {source_filename}")
    except Exception as e:
        logging.error(f"Error reading CSV file {filepath}: {e}")
        return 0

    records_to_insert = []
    for index, row in df.iterrows():
        if pd.isna(row.get('Name')) or pd.isna(row.get('Date of Admission')):
            logging.warning(f"Skipping row {index+2} in {source_filename} due to missing Name or Date of Admission.")
            continue

        record = {
            "name": clean_name(row.get('Name')),
            "age": int(row.get('Age')) if pd.notna(row.get('Age')) else None,
            "gender": str(row.get('Gender')).strip().title() if pd.notna(row.get('Gender')) else None,
            "blood_type": str(row.get('Blood Type')).strip() if pd.notna(row.get('Blood Type')) else None,
            "medical_condition": str(row.get('Medical Condition')).strip().title() if pd.notna(row.get('Medical Condition')) else None,
            "admission_date": parse_date(row.get('Date of Admission')),
            "doctor_name": clean_name(row.get('Doctor')),
            "hospital_name": str(row.get('Hospital')).strip() if pd.notna(row.get('Hospital')) else None,
            "insurance_provider": str(row.get('Insurance Provider')).strip() if pd.notna(row.get('Insurance Provider')) else None,
            "billing_amount": to_decimal128(row.get('Billing Amount')),
            "room_number": str(row.get('Room Number')).strip() if pd.notna(row.get('Room Number')) else None,
            "admission_type": str(row.get('Admission Type')).strip().title() if pd.notna(row.get('Admission Type')) else None,
            "discharge_date": parse_date(row.get('Discharge Date')),
            "medication": str(row.get('Medication')).strip().title() if pd.notna(row.get('Medication')) else None,
            "test_results": str(row.get('Test Results')).strip().title() if pd.notna(row.get('Test Results')) else None,
            "_source_filename": source_filename
        }
        records_to_insert.append(record)

    if records_to_insert:
        try:
            result = db_collection.insert_many(records_to_insert, ordered=False)
            logging.info(f"Successfully inserted {len(result.inserted_ids)} records from {source_filename} into {db_collection.name}.")
            return len(result.inserted_ids)
        except OperationFailure as e:
            logging.error(f"Error inserting records from {source_filename} into MongoDB: {e}")
            if e.details and 'writeErrors' in e.details:
                for err in e.details['writeErrors']:
                    logging.error(f"  Write error index {err['index']}: {err['errmsg']}")
            return 0
    else:
        logging.info(f"No valid records to insert from {source_filename}.")
        return 0
# --- Main ETL Logic ---
# ... (process_csv_file remains the same) ...

if __name__ == "__main__":
    logging.info("Starting ETL Process...")

    if not all([MONGO_USER, MONGO_PASS, MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COLLECTION_NAME]):
        logging.error("One or more MongoDB environment variables are not set. Exiting.")
        exit(1)

    # Get client with retries
    client = get_mongo_client(max_retries=6, retry_delay=10) # e.g. 6 retries, 10 seconds apart

    if client:
        try:
            db = client[MONGO_DB_NAME]
            collection = db[MONGO_COLLECTION_NAME]
            logging.info(f"Using database '{MONGO_DB_NAME}' and collection '{MONGO_COLLECTION_NAME}'.")

            processed_files = get_processed_files()
            csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))

            if not csv_files:
                logging.info("No CSV files found in the data directory.")
            else:
                new_files_found = False
                total_records_inserted_session = 0
                for csv_file_path in csv_files:
                    source_filename = os.path.basename(csv_file_path)
                    if source_filename not in processed_files:
                        new_files_found = True
                        logging.info(f"New file found: {source_filename}")
                        inserted_count = process_csv_file(csv_file_path, collection, source_filename)
                        if inserted_count > 0:
                            mark_file_as_processed(source_filename)
                            total_records_inserted_session += inserted_count
                        else:
                            logging.warning(f"No records were inserted from {source_filename}. It will be retried next time.")
                    else:
                        logging.info(f"Skipping already processed file: {source_filename}")
                
                if not new_files_found:
                    logging.info("No new CSV files to process in this run.")
                else:
                    logging.info(f"Total new records inserted in this session: {total_records_inserted_session}")

        except Exception as e:
            logging.error(f"An unexpected error occurred during the ETL process: {e}")
        finally:
            if client:
                client.close()
                logging.info("MongoDB connection closed.")
    else:
        logging.error("Could not establish MongoDB connection after multiple retries. ETL process cannot continue.")

    logging.info("ETL Process Finished.")