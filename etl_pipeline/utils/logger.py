"""
Logging configuration with progress tracking
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from ..config import LOG_DIR


def setup_logger(name='etl_pipeline'):
    """
    Configure logger with file and console handlers
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    logger.handlers = []
    
    # Formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter('%(message)s')
    
    # File handler - detailed logging
    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler - progress focused
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


class ProgressTracker:
    """
    Helper class for tracking and logging progress
    """
    
    def __init__(self, logger):
        self.logger = logger
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        
    def log_section(self, title, char='='):
        """Log a section header"""
        self.logger.info(f"\n{char * 60}")
        self.logger.info(title)
        self.logger.info(f"{char * 60}")
        
    def log_step(self, step_num, title):
        """Log a pipeline step"""
        self.logger.info(f"\n{'─' * 60}")
        self.logger.info(f"STEP {step_num}: {title}")
        self.logger.info(f"{'─' * 60}")
        
    def log_progress(self, message, level='info'):
        """Log a progress message with arrow indicator"""
        msg = f"   → {message}"
        if level == 'info':
            self.logger.info(msg)
        elif level == 'warning':
            self.logger.warning(f"    warning: {message}")
        elif level == 'error':
            self.logger.error(f"   error: {message}")
        elif level == 'success':
            self.logger.info(f"   success: {message}")
            
    def log_statistics(self, stats_dict):
        """Log statistics in a formatted way"""
        self.logger.info(f"\n{'─' * 60}")
        self.logger.info("📊 STATISTICS:")
        for key, value in stats_dict.items():
            self.logger.info(f"   • {key}: {value}")
        self.logger.info(f"{'─' * 60}")
        
    def final_report(self, file_name, success=True):
        """Log final pipeline report"""
        status = "COMPLETED SUCCESSFULLY" if success else "FAILED"
        success_rate = (
            (self.records_loaded / self.records_extracted * 100) 
            if self.records_extracted > 0 else 0
        )
        
        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"ETL PIPELINE {status}")
        self.logger.info(f"{'=' * 60}")
        self.logger.info(f"File: {file_name}")
        self.logger.info(f"Extracted: {self.records_extracted}")
        self.logger.info(f"Transformed: {self.records_transformed}")
        self.logger.info(f"Loaded: {self.records_loaded}")
        self.logger.info(f"Success Rate: {success_rate:.1f}%")
        self.logger.info(f"{'=' * 60}")