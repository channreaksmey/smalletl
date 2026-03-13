"""
Data loading module - handles database insertion and file archiving
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
from .config import PROCESSED_DIR, ARCHIVE_DIR
from .utils.logger import setup_logger

logger = setup_logger(__name__)


class DataLoader:
    """
    Handles loading data to database and file archiving
    """
    
    def __init__(self, db_handler, tracker=None):
        self.db_handler = db_handler
        self.tracker = tracker
        
    def load(self, df, table_name):
        """
        Load data to database and create backup
        Args:
            df: Cleaned DataFrame
            table_name: Target database table
        Returns: int - rows loaded
        """
        # Load to database
        rows_loaded = self.db_handler.load_data(df, table_name, self.tracker)
        
        if self.tracker:
            self.tracker.records_loaded = rows_loaded
        
        # Create backup
        self._create_backup(df)
        
        return rows_loaded
    
    def _create_backup(self, df):
        """Save cleaned data to processed folder"""
        if self.tracker:
            self.tracker.log_progress("Creating backup copy...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = PROCESSED_DIR / f"sales_cleaned_{timestamp}.csv"
        
        try:
            df.to_csv(backup_path, index=False)
            if self.tracker:
                self.tracker.log_progress(
                    f"Backup saved: {backup_path.name}", 
                    'success'
                )
        except Exception as e:
            if self.tracker:
                self.tracker.log_progress(
                    f"Backup failed: {e}", 
                    'warning'
                )
            logger.warning(f"Backup creation failed: {e}")
    
    def archive_file(self, file_path):
        """
        Move original file to archive folder
        Args:
            file_path: Path to original file
        """
        if self.tracker:
            self.tracker.log_progress("Archiving original file...")
        
        try:
            source = Path(file_path)
            dest = ARCHIVE_DIR / source.name
            
            # Handle duplicate filenames
            counter = 1
            while dest.exists():
                stem = source.stem
                suffix = source.suffix
                dest = ARCHIVE_DIR / f"{stem}_{counter}{suffix}"
                counter += 1
            
            source.rename(dest)
            
            if self.tracker:
                self.tracker.log_progress(
                    f"Archived to: {dest.name}", 
                    'success'
                )
            
        except Exception as e:
            if self.tracker:
                self.tracker.log_progress(f"Archive failed: {e}", 'warning')
            logger.warning(f"File archiving failed: {e}")