import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv
from bson.son import SON
from datetime import datetime

# --- Determine project root and .env path ---
# This assumes query_script.py is in a subfolder (e.g., 'app') of the project root
# If query_script.py is in the project root, change '..' to '.'
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
    DOTENV_PATH = os.path.join(PROJECT_ROOT, '.env')
except NameError:
    # __file__ is not defined if running in certain interactive environments
    # Fallback for simple cases, assuming current working directory is project root
    # and script is in a subdir like 'app'
    print("DEBUG: __file__ not defined, attempting fallback for .env path")
    DOTENV_PATH = os.path.join(os.getcwd(), '.env') # If script is run from root, and .env is in root
    # If you run 'python app/query_script.py' from root, this would need to be '../.env'
    # relative to the script. The above SCRIPT_DIR method is more robust.
    # For your case: 'python app/query_script.py' from project root:
    DOTENV_PATH = os.path.join(os.path.dirname(os.path.abspath("app/query_script.py")), '..', '.env')


print(f"DEBUG: Script directory: {SCRIPT_DIR if '__file__' in locals() else 'N/A'}")
print(f"DEBUG: Project root: {PROJECT_ROOT if '__file__' in locals() else 'N/A'}")
print(f"DEBUG: Attempting to load .env from: {DOTENV_PATH}")

# Load environment variables
success = load_dotenv(DOTENV_PATH)
print(f"DEBUG: load_dotenv success: {success}\n")

# --- Get MongoDB Connection Details ---
MONGO_USER = os.getenv('MONGO_INITDB_ROOT_USERNAME')
MONGO_PASS = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
# For an external script connecting to Docker, MONGO_HOST should be localhost
MONGO_HOST_SCRIPT = os.getenv('MONGO_HOST_SCRIPT', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
MONGO_COLLECTION_NAME = os.getenv('MONGO_COLLECTION_NAME') # Added for completeness

# --- Print Debug Information ---
print("--- DEBUG: Environment Variables ---")
print(f"DEBUG: MONGO_USER='{MONGO_USER}'")
# print(f"DEBUG: MONGO_PASS='{'*' * len(MONGO_PASS) if MONGO_PASS else None}'") # Mask password
print(f"DEBUG: MONGO_PASS (length)='{len(MONGO_PASS) if MONGO_PASS else 0}'") # Safer: print length
print(f"DEBUG: MONGO_HOST_SCRIPT='{MONGO_HOST_SCRIPT}'")
print(f"DEBUG: MONGO_PORT={MONGO_PORT}")
print(f"DEBUG: MONGO_DB_NAME='{MONGO_DB_NAME}'")
print(f"DEBUG: MONGO_COLLECTION_NAME='{MONGO_COLLECTION_NAME}'")
print("-----------------------------------\n")


def run_queries():
    if not all([MONGO_USER, MONGO_PASS, MONGO_HOST_SCRIPT, MONGO_PORT, MONGO_DB_NAME, MONGO_COLLECTION_NAME]):
        print("ERROR: One or more MongoDB connection variables are missing from .env or not loaded.")
        return

    mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST_SCRIPT}:{MONGO_PORT}/{MONGO_DB_NAME}?authSource=admin"
    print(f"DEBUG: Connecting with URI: mongodb://{MONGO_USER}:******@{MONGO_HOST_SCRIPT}:{MONGO_PORT}/{MONGO_DB_NAME}?authSource=admin\n")

    client = None  # Initialize client to None for finally block
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) # Add timeout
        client.admin.command('ismaster') # Verify connection
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        print(f"Successfully connected to MongoDB: {MONGO_HOST_SCRIPT}:{MONGO_PORT}, DB: {MONGO_DB_NAME}\n")

        # Query 1: How many patients are in the collection?
        print("--- Query 1: Total Patient Records ---")
        count = collection.count_documents({})
        print(f"Total patient records: {count}\n")

        # Query 2: List all patients admitted after January 1, 2023.
        print("--- Query 2: Patients Admitted After 2023-01-01 (First 5) ---")
        query_date = datetime(2023, 1, 1, 0, 0, 0)
        patients_after_date_cursor = collection.find({
            "admission_date": {"$gt": query_date}
        }).limit(5) # Limit to 5 for brevity in script output
        
        results_q2 = list(patients_after_date_cursor) # Convert cursor to list to check if empty
        if results_q2:
            for patient in results_q2:
                print(f"  Name: {patient.get('name')}, Admission Date: {patient.get('admission_date')}, Condition: {patient.get('medical_condition')}")
            total_matching_q2 = collection.count_documents({"admission_date": {"$gt": query_date}})
            if total_matching_q2 > 5:
                print(f"  ... and {total_matching_q2 - 5} more matching records.")
        else:
            print("  No patients found admitted after January 1, 2023.")
        print("\n")


        # Query 3a: How many patients are older than 50?
        print("--- Query 3a: Patients Older Than 50 ---")
        older_than_50_count = collection.count_documents({"age": {"$gt": 50}})
        print(f"Number of patients older than 50: {older_than_50_count}\n")

        # Query 3b: How many patients have the name "Thomas"?
        print("--- Query 3b: Patients with 'Thomas' in Name ---")
        thomas_count = collection.count_documents({"name": {"$regex": "Thomas", "$options": "i"}})
        print(f"Number of patients with 'Thomas' in their name (case-insensitive): {thomas_count}\n")

        # Query 3c: Count per each distinct Medical Condition
        print("--- Query 3c: Patient Count per Medical Condition ---")
        condition_counts = collection.aggregate([
            {"$group": {"_id": "$medical_condition", "count": {"$sum": 1}}},
            {"$sort": SON([("count", -1)])}
        ])
        for condition in condition_counts:
            print(f"  {condition['_id']}: {condition['count']}")
        print("\n")

        # Query 4: Frequency of usage for each Medication
        print("--- Query 4: Medication Frequency ---")
        medication_freq = collection.aggregate([
            {"$group": {"_id": "$medication", "frequency": {"$sum": 1}}},
            {"$sort": SON([("frequency", -1)])}
        ])
        for med in medication_freq:
            print(f"  {med['_id']}: {med['frequency']}")
        print("\n")

        # Query 5: Retrieve all patients currently taking "Lipitor"
        print("--- Query 5: Patients Taking 'Lipitor' (First 5) ---")
        target_medication = "Lipitor" # Change if needed
        lipitor_patients_cursor = collection.find({"medication": target_medication}).limit(5)
        
        results_q5 = list(lipitor_patients_cursor)
        if results_q5:
            for patient in results_q5:
                print(f"  Name: {patient.get('name')}, Condition: {patient.get('medical_condition')}")
            total_matching_q5 = collection.count_documents({"medication": target_medication})
            if total_matching_q5 > 5:
                print(f"  ... and {total_matching_q5 - 5} more matching records.")
        else:
            print(f"  No patients found taking '{target_medication}'.")
        print("\n")

    except ConnectionFailure as e:
        print(f"ERROR: Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for unexpected errors
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    run_queries()