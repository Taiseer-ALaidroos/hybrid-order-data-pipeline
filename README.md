# 🚀 Midterm Data Pipeline

## 📌 Overview
This project is an advanced and flexible Data Pipeline designed to process and clean orders data. The system utilizes smart File Routing, where large files are processed using **Apache Spark** and small files are handled via **Python Batch Processing**. Afterward, the data is cleaned, quality rules are applied, and it is stored in a **MongoDB** database across three main layers (Raw, Validated, and Quarantine).

---

## 📂 Project Structure
midterm-data-pipeline/
├── config/
│   └── settings.py              # Project settings and global variables
├── data/                        # Data folder (huge files and samples)
├── docs/                        # Documentation folder
├── reports/                     # Performance reports and processing results (JSON)
├── src/                         # Core source code
│   ├── batch_engine.py          # Batch processing engine (for small files)
│   ├── create_small_sample.py   # Tool to generate data samples
│   ├── db_loader.py             # Database connection manager
│   ├── file_router.py           # Smart router (decides the processing tool based on file size)
│   ├── main.py                  # Central orchestrator
│   ├── quality_rules.py         # Data quality and cleaning rules
│   ├── reset_db.py              # Utility to reset/drop database collections
│   ├── spark_engine.py          # Extraction and ingestion engine using PySpark
│   └── spark_etl_pipeline.py    # Advanced ELT pipeline for data processing and cleaning
├── tests/                       # Unit tests folder
├── README.md                    # Project documentation
└── requirements.txt             # Required libraries to run the project

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
* Managing sequential workflow stages (Routing ⬅️ Raw Data Ingestion ⬅️ Processing & Cleaning).

### 3. `src/file_router.py` (Smart Router)
* Calculates the actual file size and routes it automatically.
* Ensures efficiency and optimal resource consumption by routing small files (≤ 200MB) to the Python engine, and larger ones to PySpark.

### 4. `src/batch_engine.py` (Batch Engine)
* Reads small files line by line (Streaming) to save memory consumption.
* Ingests raw records in batches using `insert_many` in MongoDB to increase ingestion throughput.

### 5. `src/spark_engine.py` (Big Data Engine)
* Utilizes `SparkSession` and `mongo-spark-connector` to handle massive datasets.
* Relies on a Fixed Schema to accelerate data reading instead of exhausting the system with schema inference.
* Wraps data with Metadata to serve the ELT pipeline.

### 6. `src/spark_etl_pipeline.py` (Advanced Processing Engine)
The crown jewel of the system for Transformations:
* **Smart Cleaning:** Converts Eastern Arabic numerals to standard format, removes extra spaces, and standardizes dates.
* **Audit Trail:** Records any corrections made to the records (`raw_corrections`) to ensure transparency.
* **Classification:** Dynamically categorizes data into (`Valid`, `Corrected`, `Quarantined`).
* **Safe Ingestion:** Uses Idempotent Upsert by comparing the `order_id` to update pre-existing data instead of duplicating it.

### 7. `src/quality_rules.py` (Quality Rules)
The data firewall:
* Handles hidden file issues (like the `\ufeff` BOM character).
* Applies strict validation conditions using `Regex` (for phones and emails) and verifies the financial logic of the orders.

### 8. `src/db_loader.py` & `src/reset_db.py` (Database Utilities)
* Utilities providing a clean and unified connection to the database, alongside a script to reset collections for testing in a clean environment.

### 9. `src/create_small_sample.py` (Sampling Tool)
* A powerful CLI script to extract small samples from huge files, facilitating safe code testing.

---

## 📊 Reports & Metrics
The system generates comprehensive reports in `JSON` format inside the `reports/` folder after every run, including:
* **Time Performance:** Processing speed (Throughput) and total elapsed time in seconds.
* **Data Quality:** Counts of valid (`valid_count`), corrected (`corrected_count`), and quarantined records (`quarantine_count`).
* **Error Tracking:** Precise categorization of quarantine reasons (e.g., `MISSING_ORDER_ID`).
* **Database Operations:** Documenting the number of newly added (`inserted_count`) and updated records (`updated_count`).

---

## 🚀 How to Run
1. Ensure all required libraries are installed (`pip install -r requirements.txt`).
2. Run your local **MongoDB** server on port `27017`.
3. (Optional) If you want a completely clean environment, run `python src/reset_db.py`.
4. Start the system by running the central orchestrator:
   ```bash
   python src/main.py
