"""
ETL Pipeline Package
A modular ETL pipeline for processing daily sales data.
"""

from .pipeline import run_etl_pipeline, process_all_files

__version__ = "1.0.0"
__all__ = ["run_etl_pipeline", "process_all_files"]