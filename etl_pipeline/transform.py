"""
Data transformation and cleaning module
"""

import pandas as pd
import numpy as np
from datetime import datetime
from .utils.logger import setup_logger

logger = setup_logger(__name__)


class DataTransformer:
    """
    Handles data cleaning, standardization, and transformation
    """
    
    def __init__(self, tracker=None):
        self.tracker = tracker
        
    def transform(self, df):
        """
        Main transformation pipeline
        Args:
            df: Raw DataFrame
        Returns: Cleaned DataFrame
        """
        if self.tracker:
            self.tracker.log_step("2", "TRANSFORMATION")
            self.tracker.log_progress("Starting data cleaning...")
        
        initial_count = len(df)
        df_clean = df.copy()
        
        # Step 2.1: Standardize columns
        df_clean = self._standardize_columns(df_clean)
        
        # Step 2.2: Remove duplicates
        df_clean = self._remove_duplicates(df_clean)
        
        # Step 2.3: Handle missing values
        df_clean = self._handle_missing_values(df_clean, initial_count)
        
        # Step 2.4: Convert data types
        df_clean = self._convert_data_types(df_clean)
        
        # Step 2.5: Add metadata
        df_clean = self._add_metadata(df_clean)
        
        # Step 2.6: Standardize text
        df_clean = self._standardize_text(df_clean)
        
        # Update statistics
        final_count = len(df_clean)
        if self.tracker:
            self.tracker.records_transformed = final_count
            
            self.tracker.log_statistics({
                "Started with": f"{initial_count} rows",
                "Ended with": f"{final_count} rows",
                "Removed": f"{initial_count - final_count} rows",
                "Ready for loading": f"{final_count} clean records"
            })
        
        return df_clean
    
    def _standardize_columns(self, df):
        """Standardize column names"""
        if self.tracker:
            self.tracker.log_progress("Standardizing column names...")
        
        original = df.columns.tolist()
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.strip()
        
        if self.tracker:
            self.tracker.log_progress(
                f"Renamed {len(original)} columns", 
                'success'
            )
        
        return df
    
    def _remove_duplicates(self, df):
        """Remove duplicate rows"""
        if self.tracker:
            self.tracker.log_progress("Checking for duplicates...")
        
        dups = df.duplicated().sum()
        if dups > 0:
            df = df.drop_duplicates()
            if self.tracker:
                self.tracker.log_progress(f"Removed {dups} duplicates", 'success')
        else:
            if self.tracker:
                self.tracker.log_progress("No duplicates found", 'success')
        
        return df
    
    def _handle_missing_values(self, df, initial_count):
        """Handle missing and invalid data"""
        if self.tracker:
            self.tracker.log_progress("Handling missing values...")
        
        # Critical columns that must have values
        critical_cols = ['sale_id', 'date', 'amount']
        available_critical = [col for col in critical_cols if col in df.columns]
        
        # Drop rows with missing critical data
        df_clean = df.dropna(subset=available_critical)
        dropped = initial_count - len(df_clean)
        
        if dropped > 0:
            if self.tracker:
                self.tracker.log_progress(
                    f"Dropped {dropped} rows with missing critical data", 
                    'success'
                )
        else:
            if self.tracker:
                self.tracker.log_progress("All critical fields present", 'success')
        
        # Fill optional fields with defaults
        optional_defaults = {
            'customer_name': 'Unknown',
            'product': 'Unspecified',
            'region': 'Unknown',
            'sales_rep': 'Unknown'
        }
        
        filled_count = 0
        for col, default in optional_defaults.items():
            if col in df_clean.columns:
                null_count = df_clean[col].isnull().sum()
                if null_count > 0:
                    df_clean[col] = df_clean[col].fillna(default)
                    filled_count += null_count
        
        if filled_count > 0 and self.tracker:
            self.tracker.log_progress(
                f"Filled {filled_count} missing optional values", 
                'success'
            )
        
        return df_clean
    
    def _convert_data_types(self, df):
        """Convert columns to proper data types"""
        if self.tracker:
            self.tracker.log_progress("Converting data types...")
        
        # Date columns
        date_cols = [col for col in df.columns if 'date' in col]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            valid = df[col].notna().sum()
            if self.tracker:
                self.tracker.log_progress(
                    f"Date column '{col}': {valid} valid dates", 
                    'success'
                )
        
        # Numeric columns
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            negatives = (df['amount'] < 0).sum()
            if negatives > 0:
                df = df[df['amount'] >= 0]
                if self.tracker:
                    self.tracker.log_progress(
                        f"Removed {negatives} negative amounts", 
                        'warning'
                    )
            else:
                if self.tracker:
                    self.tracker.log_progress("Amounts validated", 'success')
        
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
            df['quantity'] = df['quantity'].fillna(0).astype(int)
            if self.tracker:
                self.tracker.log_progress("Quantity converted to integer", 'success')
        
        return df
    
    def _add_metadata(self, df):
        """Add processing metadata"""
        if self.tracker:
            self.tracker.log_progress("Adding metadata...")
        
        df['processed_at'] = datetime.now()
        df['source_file'] = 'daily_sales'
        
        if self.tracker:
            self.tracker.log_progress(
                "Added 'processed_at' and 'source_file'", 
                'success'
            )
        
        return df
    
    def _standardize_text(self, df):
        """Clean and standardize text fields"""
        if self.tracker:
            self.tracker.log_progress("Standardizing text fields...")
        
        text_cols = ['customer_name', 'product', 'region', 'sales_rep']
        standardized = 0
        
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()
                standardized += 1
        
        if self.tracker:
            self.tracker.log_progress(
                f"Standardized {standardized} text columns", 
                'success'
            )
        
        return df
    
    def validate(self, df):
        """
        Run data quality validation checks
        Returns: bool (True if passed, False if warnings)
        """
        if self.tracker:
            self.tracker.log_step("2.5", "VALIDATION")
            self.tracker.log_progress("Running quality checks...")
        
        issues = []
        
        # Check 1: Empty dataframe
        if df.empty:
            issues.append("DataFrame is empty")
        
        # Check 2: Future dates
        date_cols = [col for col in df.columns if 'date' in col]
        for col in date_cols:
            future = df[col] > datetime.now()
            if future.any():
                issues.append(f"Future dates in '{col}'")
        
        # Check 3: Amount outliers
        if 'amount' in df.columns:
            q99 = df['amount'].quantile(0.99)
            outliers = df[df['amount'] > q99 * 10]
            if len(outliers) > 0:
                issues.append(f"{len(outliers)} amount outliers (>10x 99th percentile)")
        
        # Report results
        if issues:
            if self.tracker:
                self.tracker.log_progress("Validation warnings:", 'warning')
                for issue in issues:
                    self.tracker.logger.info(f"      - {issue}")
                self.tracker.log_progress("Proceeding with load", 'warning')
            return False
        else:
            if self.tracker:
                self.tracker.log_progress("All checks passed", 'success')
            return True