# Daily Sales ETL Pipeline

A production-ready Python ETL (Extract, Transform, Load) pipeline designed for automated processing of daily sales CSV files into a MySQL database.

## Features

- **Extract**: Reads daily sales CSV files from input directory
- **Transform**: Cleans data (handles missing values, duplicates, type conversions, standardization)
- **Validate**: Data quality checks for dates, amounts, and outliers
- **Load**: Inserts cleaned records into MySQL Workbench with automatic table creation
- **Archive**: Moves processed files to archive folder and creates backup copies

## Tech Stack

- Python 3.x
- pandas (data manipulation)
- SQLAlchemy (database ORM)
- PyMySQL (MySQL connection)
- cryptography (authentication)

## Quick Start

1. Install dependencies:
   ```bash
   pip install pandas sqlalchemy pymysql cryptography
   ```
2. Configure database credentials in db_config 
3. Add CSV files to input_sales/ folder 
4. Run pipeline:
   ```bash
   python etl_pipeline.py
   ```

## Project Structure
   ```
   ├── etl_pipeline.py      # Main ETL script
   ├── input_sales/         # Drop daily CSV files here
   ├── processed/          # Cleaned data backups
   ├── archive/            # Original files after processing
   └── etl_pipeline.log    # Execution logs
   ```

## Database Schema

MySQL table daily_sales includes:
* Transaction details (sale_id, date, customer, product)
* Financial data (amount, quantity)
* Metadata (processed_at timestamp, source file)
* Indexed for fast querying

## Logging & Monitoring

Comprehensive logging tracks:
* Extraction row counts
* Data quality issues
* Load success/failure
* File archiving status

Perfect for small business data automation or learning ETL fundamentals.

---

## **GitHub Topics/Tags**

etl-pipeline python mysql pandas data-engineering automation sales-data csv-processing sqlalchemy data-cleaning

