#!/usr/bin/env python3
"""
ETL Pipeline Runner
Entry point for executing the ETL pipeline
"""

from etl_pipeline.pipeline import process_all_files, run_etl_pipeline
from etl_pipeline.config import INPUT_DIR
from pathlib import Path
import sys


def main():
    """
    Main execution function
    """
    print("=" * 60)
    print("DAILY SALES ETL PIPELINE")
    print("=" * 60)
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        # Process specific file
        file_path = sys.argv[1]
        print(f"Processing single file: {file_path}")
        success = run_etl_pipeline(file_path)
        sys.exit(0 if success else 1)
    else:
        # Process all files in input directory
        print(f"Processing all files in: {INPUT_DIR}")
        process_all_files()


if __name__ == "__main__":
    main()