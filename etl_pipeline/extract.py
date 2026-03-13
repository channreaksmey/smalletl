"""
Data extraction module - handles CSV file reading
"""

import pandas as pd
from pathlib import Path
from .utils.logger import setup_logger

logger = setup_logger(__name__)


class DataExtractor:
    """
    Handles extraction of data from source files
    """
    
    def __init__(self, tracker=None):
        self.tracker = tracker
        
    def extract_csv(self, file_path):
        """
        Extract data from CSV file
        Args:
            file_path: Path to CSV file
        Returns: pd.DataFrame
        """
        if self.tracker:
            self.tracker.log_step("1", "EXTRACTION")
            self.tracker.log_progress(f"Reading file: {file_path}")
        
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            
            # Update tracker statistics
            if self.tracker:
                self.tracker.records_extracted = len(df)
                self.tracker.log_progress(f"Parsed CSV successfully", 'success')
                self.tracker.log_progress(
                    f"Extracted {len(df)} rows, {len(df.columns)} columns"
                )
                self.tracker.log_progress(
                    f"Columns: {', '.join(df.columns.tolist())}"
                )
                
                # Show sample if not empty
                if not df.empty:
                    self.tracker.logger.info(f"\n   Sample data (first 2 rows):")
                    sample = df.head(2).to_string().replace('\n', '\n      ')
                    self.tracker.logger.info(f"      {sample}")
            
            # Validation
            if df.empty:
                if self.tracker:
                    self.tracker.log_progress("Warning: CSV file is empty", 'warning')
                else:
                    logger.warning("CSV file is empty")
            
            return df
            
        except FileNotFoundError:
            error_msg = f"File not found: {file_path}"
            if self.tracker:
                self.tracker.log_progress(error_msg, 'error')
            logger.error(error_msg)
            raise
            
        except pd.errors.EmptyDataError:
            error_msg = f"CSV file is empty: {file_path}"
            if self.tracker:
                self.tracker.log_progress(error_msg, 'error')
            logger.error(error_msg)
            raise
            
        except Exception as e:
            error_msg = f"Extraction failed: {e}"
            if self.tracker:
                self.tracker.log_progress(error_msg, 'error')
            logger.error(error_msg)
            raise