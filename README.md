# Medical Data Migration to MongoDB Project

This project demonstrates a pipeline for ingesting medical data from CSV files into MongoDB, using Docker for containerization and Python for the ETL process.

## 🚀 Features

*   Automated ETL process to load CSV data into MongoDB.
*   Dockerized multi-container setup (Python application + MongoDB).
*   Tracks processed files to prevent duplicate ingestion.
*   Example queries for data exploration.

## 🛠️ Project Structure

```
medical_data_migration/
├── .env.example        # Example environment variables
├── .gitignore          # Specifies intentionally untracked files
├── app/
│   ├── Dockerfile        # Dockerfile for the Python ETL application
│   ├── etl_script.py     # Python script for ETL
│   └── requirements.txt  # Python dependencies
│   └── query_script.py   # (Optional) Python script for running queries
├── data/
│   └── healthcare_dataset-20250506.csv # Sample dataset
│   └── processed_files.log # Log of processed files (if committed)
├── docker-compose.yml  # Docker Compose configuration
└── README.md             # This file
```

## ⚙️ Prerequisites

*   [Docker](https://www.docker.com/get-started) installed and running.
*   [Docker Compose](https://docs.docker.com/compose/install/) (usually included with Docker Desktop).
*   Git.

## 📝 Setup and Usage

1.  **Clone the repository:**
    ```bash
    git clone <your_repository_link>
    cd medical_data_migration
    ```

2.  **Configure Environment Variables:**
    *   Copy the example environment file:
    ```bash
    cp .env.example .env
    ```
    *   Edit the `.env` file and provide your desired MongoDB credentials (especially `MONGO_INITDB_ROOT_PASSWORD`). The defaults are often fine for local development.

3.  **Build and Run the Application:**
    *   To build the Docker images and start the services:
    ```bash
    docker-compose up --build
    ```
    *   This will start the MongoDB instance and run the Python ETL script, which processes any new CSV files in the `./data` directory.
    *   To run in detached mode (in the background):
    ```bash
    docker-compose up --build -d
    ```

4.  **Accessing MongoDB Data:**
    *   You can connect to the MongoDB instance using MongoDB Compass or `mongosh` with the following details:
        *   **Host:** `localhost`
        *   **Port:** `27017`
        *   **Username:** (The one you set for `MONGO_INITDB_ROOT_USERNAME` in `.env`)
        *   **Password:** (The one you set for `MONGO_INITDB_ROOT_PASSWORD` in `.env`)
        *   **Authentication Database:** `admin`
        *   **Application Database Name:** `medicaldb` (or as set in `.env`)
        *   **Collection Name:** `medical_records` (or as set in `.env`)
    *   To access `mongosh` via Docker:
    ```bash
    docker exec -it mongodb_container mongosh -u <your_username> -p <your_password> --authenticationDatabase admin medicaldb
    ```

5.  **Adding New Data:**
    *   Place new CSV files (following the same structure and naming convention like `filename-YYYYMMDD.csv`) into the `./data` folder.
    *   Run `docker-compose up --build` again. The ETL script will detect and process only the new files.

6.  **Stopping the Application:**
    ```bash
    docker-compose down
    ```
    To also remove the MongoDB data volume (for a complete reset):
    ```bash
    docker-compose down -v
    ```

## 📄 MongoDB Schema

The data is stored in a single collection named `medical_records` within the `medicaldb` database. Each document represents a record from the CSV file.

**Example Document:**
```json
{
"_id": "ObjectId(...)",
"name": "Bobby Jackson",
"age": 30,
"gender": "Male",
"blood_type": "B-",
"medical_condition": "Cancer",
"admission_date": "ISODate(\"2024-01-31T00:00:00Z\")",
"doctor_name": "Matthew Smith",
"hospital_name": "Sons and Miller",
"insurance_provider": "Blue Cross",
"billing_amount": "NumberDecimal(\"18856.28\")",
"room_number": "328",
"admission_type": "Urgent",
"discharge_date": "ISODate(\"2024-02-02T00:00:00Z\")",
"medication": "Paracetamol",
"test_results": "Normal",
"_source_filename": "healthcare_dataset-20250506.csv"
}
```

## 🔍 Example Queries

1.  **Count total records:**
    ```javascript
    // In mongosh, after 'use medicaldb;'
    db.medical_records.countDocuments({});
    ```
2.  **Find patients older than 50:**
    ```javascript
    db.medical_records.find({ "age": { "$gt": 50 } });
    ```

## ✍️ Author

Yacine Ammi
