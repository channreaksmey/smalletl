"""
Main ETL pipeline orchestrator
"""

from pathlib import Path
from .config import INPUT_DIR, DB_CONFIG, TABLE_NAME
from .database import DatabaseHandler
from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader
from .utils.logger import setup_logger, ProgressTracker

logger = setup_logger(__name__)


class ETLPipeline:
    """
    Main pipeline class that orchestrates ETL process
    """
    
    def __init__(self):
        self.db_handler = DatabaseHandler()
        self.tracker = ProgressTracker(logger)
        self.extractor = DataExtractor(self.tracker)
        self.transformer = DataTransformer(self.tracker)
        self.loader = None  # Initialized after DB check
        
    def initialize(self):
        """Initialize pipeline components"""
        # Test database connection
        if not self.db_handler.test_connection(self.tracker):
            logger.error("Pipeline initialization failed")
            return False
        
        # Initialize loader with db_handler
        self.loader = DataLoader(self.db_handler, self.tracker)
        
        self.tracker.log_section("Pipeline Ready", '=')
        return True
    
    def run_single_file(self, file_path):
        """
        Run complete ETL pipeline for single file
        Args:
            file_path: Path to CSV file
        Returns: bool - success status
        """
        file_name = Path(file_path).name
        
        self.tracker.log_section(
            f"PROCESSING: {file_name}", 
            '='
        )
        
        try:
            # Step 1: Extract
            raw_data = self.extractor.extract_csv(file_path)
            
            # Step 2: Transform
            clean_data = self.transformer.transform(raw_data)
            
            # Step 2.5: Validate
            self.transformer.validate(clean_data)
            
            # Step 3: Load
            self.loader.load(clean_data, TABLE_NAME)
            
            # Success
            self.tracker.final_report(file_name, success=True)
            
            # Archive original file
            self.loader.archive_file(file_path)
            
            return True
            
        except Exception as e:
            self.tracker.final_report(file_name, success=False)
            logger.error(f"Pipeline failed for {file_name}: {e}")
            return False
    
    def run_batch(self):
        """
        Process all CSV files in input directory
        """
        self.tracker.log_section("SCANNING FOR FILES", '=')
        self.tracker.log_progress(f"Looking in: {INPUT_DIR}")
        
        csv_files = list(INPUT_DIR.glob('*.csv'))
        
        if not csv_files:
            self.tracker.log_progress("No CSV files found!", 'warning')
            self.tracker.log_progress("Add files to 'input_sales/' folder")
            return
        
        self.tracker.log_progress(f"Found {len(csv_files)} file(s)", 'success')
        
        successful = 0
        failed = 0
        
        for i, csv_file in enumerate(csv_files, 1):
            self.tracker.logger.info(f"\n{'=' * 60}")
            self.tracker.logger.info(f"FILE {i} of {len(csv_files)}")
            self.tracker.logger.info(f"{'=' * 60}")
            
            success = self.run_single_file(str(csv_file))
            
            if success:
                successful += 1
            else:
                failed += 1
        
        # Final batch report
        self.tracker.log_section("BATCH PROCESSING COMPLETE", '=')
        self.tracker.log_statistics({
            "Total files": len(csv_files),
            "Successful": successful,
            "Failed": failed,
            "Success rate": f"{(successful/len(csv_files)*100):.1f}%"
        })


# Convenience functions for direct use
def run_etl_pipeline(file_path):
    """
    Run ETL pipeline for single file
    Args:
        file_path: Path to CSV file
    """
    pipeline = ETLPipeline()
    
    if not pipeline.initialize():
        return False
    
    return pipeline.run_single_file(file_path)


def process_all_files():
    """
    Process all files in input directory
    """
    pipeline = ETLPipeline()
    
    if not pipeline.initialize():
        return
    
    pipeline.run_batch()