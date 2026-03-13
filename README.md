# Modular Sales ETL Pipeline

A production-ready, modular ETL (Extract, Transform, Load) pipeline built with clean architecture principles. Designed for automated processing of daily sales CSV files into MySQL databases with comprehensive logging and error handling.

## Architecture

This pipeline follows **modular design patterns** with separated concerns:
```bash
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Extract   │────▶│  Transform  │────▶│   Validate  │────▶│    Load     │
│  (extract)  │     │ (transform) │     │  (validate) │     │   (load)    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
│                   │                   │                   │
└───────────────────┴───────────────────┴───────────────────┘
│
┌──────────────┐
│  Pipeline    │
│(orchestrator)│
└──────────────┘
```


## Project Structure

```bash
etl_pipeline/
├── extract.py          # CSV extraction logic
├── transform.py        # Data cleaning & standardization
├── load.py             # Database insertion & archiving
├── database.py         # Connection management
├── pipeline.py         # Main orchestrator
├── config.py           # Centralized configuration
└── utils/
└── logger.py       # Progress tracking & logging
```

## Key Features

| Feature | Description |
|---------|-------------|
| **Modular Components** | Each ETL step is independent, testable, and reusable |
| **Progress Logging** | Visual step-by-step progress with detailed statistics |
| **Data Validation** | Quality checks between transform and load phases |
| **Error Handling** | Graceful failures with detailed error context |
| **Automatic Archiving** | Moves processed files to archive, creates backups |
| **Batch Processing** | Handles single files or entire directories |
| **Database Agnostic** | Easy to switch from MySQL to PostgreSQL, etc. |

## Quick Start

### 1. Install Dependencies

```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline
python run.py

# Or process specific file
python run.py input_sales/data.csv
```
### 2. Set Up MySQL Workbench

* Open MySQL Workbench and connect to your local instance
* Run `db.sql` in the Query tab

### 3. Configure Pipeline
Edit etl_pipeline/config.py with your credentials:

```bash
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',                    # Your MySQL username
    'password': 'YOUR_PASSWORD_HERE',  # Your MySQL password
    'database': 'sales_db',
    'port': 3306
}
```
### 4. Prepare Input Data

Create a CSV file in `input_sales/` folder. For example:
```bash
sale_id,date,customer_name,product,amount,quantity,region,sales_rep
S001,2024-01-15,Acme Corp,Widget A,1500.00,10,North,John Doe
S002,2024-01-15,TechStart Inc,Widget B,2300.50,5,South,Jane Smith
```
### 5. Run Pipeline
```bash
# Process all files in input_sales/
python run.py

# Or process specific file
python run.py input_sales/sales_2024_01_15.csv
```

## Sample Output

```bash
============================================================
STEP 1: EXTRACTION
============================================================
   → Reading file: input_sales/sales_2024_01_15.csv
   ✅ Parsed CSV successfully
   → Extracted 5 rows, 8 columns
   → Columns: sale_id, date, customer_name, product, amount...

============================================================
STEP 2: TRANSFORMATION
============================================================
   → Standardizing column names...
   ✅ Renamed 8 columns
   → Checking for duplicates...
   ✅ No duplicates found
   → Handling missing values...
   ✅ All critical fields present

📊 STATISTICS:
   • Started with: 5 rows
   • Ended with: 5 rows
   • Ready for loading: 5 clean records

============================================================
✅ ETL PIPELINE COMPLETED SUCCESSFULLY
📈 Success Rate: 100.0%
============================================================
```

## Tech Stack

* *Python 3.10* - Core language
* *pandas* - Data manipulation and analysis
* *SQLAlchemy* - Database ORM and connection management
* *PyMySQL* - MySQL database driver
* *cryptography* - Secure authentication handling

---

## **GitHub Topics/Tags**

etl-pipeline python pandas sqlalchemy data-engineering modular-architecture clean-code data-transformation mysql data-warehouse automation

---