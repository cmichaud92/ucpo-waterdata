"""
Initial Data Load Script

This script performs a complete historical data load for all configured sites and parameters.
It should be run once to populate an empty data lake with historical data.

Usage:
    python scripts/initial_load.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
"""
import sys
import argparse
import logging
import datetime as dt
from pathlib import Path

# Add project root's parent to python path so 'src' can be imported
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.database.connection import (
    connect_duckdb,
    execute_sql_script,
    write_meta_tables_to_csv,
    fetch_site_parameters
)
from src.etl.extractors import NWISExtractor
from src.etl.transformers import NWISTransformer
from src.etl.loaders import DataLakeLoader
from src.utils.logging_config import setup_logging


class InitialDataLoader:
    """Class to handle the initial data load process."""

    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date
        self.nwis_extractor = NWISExtractor()
        # self.hdb_extractor = HDBExtractor()  # Placeholder for future use
        self.transformer = NWISTransformer()
        # self.hdb_transformer = HDBTransformer()  # Placeholder for future use
        self.loader = DataLakeLoader()

        # Statistics
        self.sites_processed = 0
        self.sites_failed = 0
        self.total_records = 0

    def initialize_database(self):
        """Initialize the DuckDB database schema and metadata tables."""
        logging.info("🔧 Initializing database schema...")

        try:
            # Create database schema
            schema_file = PROJECT_ROOT / 'sql' / 'schema.sql'
            if schema_file.exists():
                execute_sql_script(schema_file)
                logging.info("✅ Database schema created successfully")
            else:
                logging.warning(f"⚠️ Schema file not found: {schema_file}")

            # Create views
            views_file = PROJECT_ROOT / 'sql' / 'views.sql'
            if views_file.exists():
                execute_sql_script(views_file)
                logging.info("✅ Database views created successfully")

        except Exception as e:
            logging.error(f"❌ Error initializing database: {e}")
            raise

    # write a helper function in the connection module to load metadata tables from CSV files
    def load_metadata(self):
        """Load metadata tables from CSV files stored in the artifacts directory."""
        logging.info("📥 Loading metadata tables...")
        write_meta_tables_to_csv()