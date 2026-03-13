"""
Configuration settings for ETL pipeline
"""

from pathlib import Path

# Database configuration - UPDATE THESE
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Smey_2603',
    'database': 'sales_db',
    'port': 3306
}

# Directory paths
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / 'input_sales'
PROCESSED_DIR = BASE_DIR / 'processed'
ARCHIVE_DIR = BASE_DIR / 'archive'
LOG_DIR = BASE_DIR / 'logs'

# Ensure directories exist
for directory in [INPUT_DIR, PROCESSED_DIR, ARCHIVE_DIR, LOG_DIR]:
    directory.mkdir(exist_ok=True)

# Pipeline settings
BATCH_SIZE = 1000
TABLE_NAME = 'daily_sales'