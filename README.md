# 🚀 Midterm Data Pipeline

## 📌 Overview
This project is an advanced and flexible Data Pipeline designed to process and clean orders data. The system utilizes smart File Routing, where large files are processed using Apache Spark and small files are handled via Python Batch Processing. Afterward, the data is cleaned, quality rules are applied, and it is stored in a MongoDB database across three main layers (Raw, Validated, and Quarantine).

---

## 📂 Project Structure
- `config/settings.py`: Project settings and global variables
- `src/batch_engine.py`: Batch processing engine (for small files)
- `src/create_small_sample.py`: Tool to generate data samples
- `src/db_loader.py`: Database connection manager
- `src/file_router.py`: Smart router (decides the processing tool based on file size)
- `src/main.py`: Central orchestrator
- `src/quality_rules.py`: Data quality and cleaning rules
- `src/reset_db.py`: Utility to reset/drop database collections
- `src/spark_engine.py`: Extraction and ingestion engine using PySpark
- `src/spark_etl_pipeline.py`: Advanced ELT pipeline for data processing and cleaning

---

## 🛠️ Modules & Engines

### 1. `config/settings.py` (Central Configurations)
The beating heart of the project, containing:
* Paths for data and reports directories.
* Routing configurations (`SMALL_FILE_THRESHOLD_MB = 200.0`) to switch between Python and Spark.
* MongoDB connection strings and collection names (`orders_raw`, `orders_validated`, `orders_quarantine`).

### 2. `src/main.py` (Central Orchestrator)
The main entry point to run the project, featuring:
* Interactive prompt for the user to input the file path, with smart handling of Windows paths.
* Process Isolation using `subprocess` to prevent memory overlaps.
* Managing sequential workflow stages.

### 3. `src/file_router.py` (Smart Router)
* Calculates the actual file size and routes it automatically.
* Ensures efficiency by routing small files (≤ 200MB) to the Python engine, and larger ones to PySpark.

### 4. `src/batch_engine.py` (Batch Engine)
* Reads small files line by line to save memory consumption.
* Ingests raw records in batches using `insert_many` in MongoDB to increase throughput.

### 5. `src/spark_engine.py` (Big Data Engine)
* Utilizes `SparkSession` and `mongo-spark-connector` to handle massive datasets.
* Relies on a Fixed Schema to accelerate data reading.
* Wraps data with Metadata to serve the ELT pipeline.

### 6. `src/spark_etl_pipeline.py` (Advanced Processing Engine)
The crown jewel of the system for Transformations:
* **Smart Cleaning:** Converts Eastern Arabic numerals, removes extra spaces, and standardizes dates.
* **Audit Trail:** Records any corrections made to the records to ensure transparency.
* **Classification:** Dynamically categorizes data into Valid, Corrected, and Quarantined.
* **Safe Ingestion:** Uses Idempotent Upsert by comparing the `order_id` to update pre-existing data instead of duplicating it.

### 7. `src/quality_rules.py` (Quality Rules)
The data firewall:
* Handles hidden file issues (like the `\ufeff` BOM character).
* Applies strict validation conditions using Regex and verifies the financial logic.

---

## 🚀 How to Run
1. Ensure all required libraries are installed (`pip install -r requirements.txt`).
2. Run your local MongoDB server on port `27017`.
3. Start the system by running the central orchestrator:
   ```bash
   python src/main.py# 🚀 Midterm Data Pipeline

## 📌 Overview
This project is an advanced and flexible Data Pipeline designed to process and clean orders data. The system utilizes smart File Routing, where large files are processed using Apache Spark and small files are handled via Python Batch Processing. Afterward, the data is cleaned, quality rules are applied, and it is stored in a MongoDB database across three main layers (Raw, Validated, and Quarantine).

---

## 📂 Project Structure
- `config/settings.py`: Project settings and global variables
- `src/batch_engine.py`: Batch processing engine (for small files)
- `src/create_small_sample.py`: Tool to generate data samples
- `src/db_loader.py`: Database connection manager
- `src/file_router.py`: Smart router (decides the processing tool based on file size)
- `src/main.py`: Central orchestrator
- `src/quality_rules.py`: Data quality and cleaning rules
- `src/reset_db.py`: Utility to reset/drop database collections
- `src/spark_engine.py`: Extraction and ingestion engine using PySpark
- `src/spark_etl_pipeline.py`: Advanced ELT pipeline for data processing and cleaning

---

## 🛠️ Modules & Engines

### 1. `config/settings.py` (Central Configurations)
The beating heart of the project, containing:
* Paths for data and reports directories.
* Routing configurations (`SMALL_FILE_THRESHOLD_MB = 200.0`) to switch between Python and Spark.
* MongoDB connection strings and collection names (`orders_raw`, `orders_validated`, `orders_quarantine`).

### 2. `src/main.py` (Central Orchestrator)
The main entry point to run the project, featuring:
* Interactive prompt for the user to input the file path, with smart handling of Windows paths.
* Process Isolation using `subprocess` to prevent memory overlaps.
* Managing sequential workflow stages.

### 3. `src/file_router.py` (Smart Router)
* Calculates the actual file size and routes it automatically.
* Ensures efficiency by routing small files (≤ 200MB) to the Python engine, and larger ones to PySpark.

### 4. `src/batch_engine.py` (Batch Engine)
* Reads small files line by line to save memory consumption.
* Ingests raw records in batches using `insert_many` in MongoDB to increase throughput.

### 5. `src/spark_engine.py` (Big Data Engine)
* Utilizes `SparkSession` and `mongo-spark-connector` to handle massive datasets.
* Relies on a Fixed Schema to accelerate data reading.
* Wraps data with Metadata to serve the ELT pipeline.

### 6. `src/spark_etl_pipeline.py` (Advanced Processing Engine)
The crown jewel of the system for Transformations:
* **Smart Cleaning:** Converts Eastern Arabic numerals, removes extra spaces, and standardizes dates.
* **Audit Trail:** Records any corrections made to the records to ensure transparency.
* **Classification:** Dynamically categorizes data into Valid, Corrected, and Quarantined.
* **Safe Ingestion:** Uses Idempotent Upsert by comparing the `order_id` to update pre-existing data instead of duplicating it.

### 7. `src/quality_rules.py` (Quality Rules)
The data firewall:
* Handles hidden file issues (like the `\ufeff` BOM character).
* Applies strict validation conditions using Regex and verifies the financial logic.

---

## 🚀 How to Run
1. Ensure all required libraries are installed (`pip install -r requirements.txt`).
2. Run your local MongoDB server on port `27017`.
3. Start the system by running the central orchestrator:
   ```bash
   python src/main.py
