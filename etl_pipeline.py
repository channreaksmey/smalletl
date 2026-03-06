import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import os
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SalesETLPipeline:
    def __init__(self, db_config):
        """
        Initialize ETL pipeline with database configuration
        
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'Smey_2603',
            'database': 'sales_db',
            'port': 3306
        }
        """
        self.db_config = db_config
        self.engine = self._create_engine()
        self.processed_dir = Path('processed')
        self.processed_dir.mkdir(exist_ok=True)
        
    def _create_engine(self):
        """Create SQLAlchemy engine for MySQL connection"""
        connection_string = (
            f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        return create_engine(connection_string)
    
    def extract(self, csv_path):
        """Extract data from CSV file"""
        logger.info(f"Extracting data from {csv_path}")
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Extracted {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
    
    def transform(self, df):
        """Clean and transform sales data"""
        logger.info("Starting data transformation")
        
        # Create copy to avoid modifying original
        df_clean = df.copy()
        
        # 1. Standardize column names (lowercase, no spaces)
        df_clean.columns = df_clean.columns.str.lower().str.replace(' ', '_')
        
        # 2. Remove duplicates
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        logger.info(f"Removed {initial_rows - len(df_clean)} duplicate rows")
        
        # 3. Handle missing values
        # Drop rows where critical fields are missing
        critical_cols = ['sale_id', 'date', 'amount']
        df_clean = df_clean.dropna(subset=[col for col in critical_cols if col in df_clean.columns])
        
        # Fill optional fields with defaults
        if 'customer_name' in df_clean.columns:
            df_clean['customer_name'] = df_clean['customer_name'].fillna('Unknown')
        if 'product' in df_clean.columns:
            df_clean['product'] = df_clean['product'].fillna('Unspecified')
        
        # 4. Data type conversions
        # Convert date columns
        date_cols = [col for col in df_clean.columns if 'date' in col]
        for col in date_cols:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        # Convert numeric columns
        if 'amount' in df_clean.columns:
            df_clean['amount'] = pd.to_numeric(df_clean['amount'], errors='coerce')
            # Remove negative amounts (data quality issue)
            df_clean = df_clean[df_clean['amount'] >= 0]
        
        if 'quantity' in df_clean.columns:
            df_clean['quantity'] = pd.to_numeric(df_clean['quantity'], errors='coerce').fillna(0)
        
        # 5. Add metadata columns
        df_clean['processed_at'] = datetime.now()
        df_clean['source_file'] = 'daily_sales'
        
        # 6. Standardize text fields
        text_cols = ['customer_name', 'product', 'region', 'sales_rep']
        for col in text_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip().str.title()
        
        logger.info(f"Transformation complete. {len(df_clean)} rows ready for loading")
        return df_clean
    
    def load(self, df, table_name='daily_sales'):
        """Load data into MySQL database"""
        logger.info(f"Loading data into table: {table_name}")
        
        try:
            # Create table if not exists (append mode for daily loads)
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                method='multi'
            )
            
            logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
            
            # Archive processed file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_path = self.processed_dir / f"sales_{timestamp}.csv"
            df.to_csv(archive_path, index=False)
            logger.info(f"Archived processed data to {archive_path}")
            
        except Exception as e:
            logger.error(f"Loading failed: {e}")
            raise
    
    def validate_data(self, df):
        """Run data quality checks"""
        logger.info("Running data validation checks")
        
        issues = []
        
        # Check 1: No empty dataframe
        if df.empty:
            issues.append("DataFrame is empty")
        
        # Check 2: Date range validation
        date_cols = [col for col in df.columns if 'date' in col]
        for col in date_cols:
            future_dates = df[col] > datetime.now()
            if future_dates.any():
                issues.append(f"Future dates found in {col}")
        
        # Check 3: Amount outliers (optional business rule)
        if 'amount' in df.columns:
            q99 = df['amount'].quantile(0.99)
            outliers = df[df['amount'] > q99 * 10]
            if len(outliers) > 0:
                issues.append(f"Found {len(outliers)} potential amount outliers")
        
        if issues:
            for issue in issues:
                logger.warning(f"Validation issue: {issue}")
        else:
            logger.info("All validation checks passed")
        
        return len(issues) == 0
    
    def run_pipeline(self, csv_path, table_name='daily_sales'):
        """Execute full ETL pipeline"""
        logger.info("=" * 50)
        logger.info("STARTING ETL PIPELINE")
        logger.info("=" * 50)
        
        try:
            # Extract
            raw_data = self.extract(csv_path)
            
            # Transform
            clean_data = self.transform(raw_data)
            
            # Validate
            if not self.validate_data(clean_data):
                logger.warning("Validation warnings present, proceeding with caution")
            
            # Load
            self.load(clean_data, table_name)
            
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            return True
            
        except Exception as e:
            logger.error(f"ETL PIPELINE FAILED: {e}")
            return False


# Example usage and automation script
def main():
    # Database configuration - update with your credentials
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Smey_2603',
        'database': 'sales_db',
        'port': 3306
    }
    
    # Initialize pipeline
    pipeline = SalesETLPipeline(db_config)
    
    # Process all CSV files in input directory
    input_dir = Path('input_sales')
    input_dir.mkdir(exist_ok=True)
    
    csv_files = list(input_dir.glob('*.csv'))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {input_dir}")
        return
    
    for csv_file in csv_files:
        success = pipeline.run_pipeline(str(csv_file))
        
        if success:
            # Move processed file to archive
            archive_dir = Path('archive')
            archive_dir.mkdir(exist_ok=True)
            csv_file.rename(archive_dir / csv_file.name)
            logger.info(f"Moved {csv_file.name} to archive")

if __name__ == "__main__":
    main()