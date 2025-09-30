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

    def load_nwis_data(self):
        """Load historical NWIS data for all sites and parameters."""
        logging.info("🌊 Starting NWIS data load...")

        try:
            with connect_duckdb() as con:
                # Get all NWIS sites
                nwis_sites = con.execute("""
                                         SELECT s.site_id, s.site_cd, s.site_nm
                                         FROM site s
                                         INNER JOIN source src ON s.source_id = src.source_id
                                         WHERE src.source_cd = 'NWIS'
                                         ORDER BY s.site_cd
                                         """).fetchall()
            
            logging.info(f"📍 Found {len(nwis_sites)} NWIS sites to process")

            for site_id, site_code, site_name in nwis_sites:
                self._process_nwis_site(site_id, site_code, site_name)

        except Exception as e:
            logging.error(f"❌ Error in NWIS data load: {e}")
            raise

    def load_hdb_data(self):
        """Load historical HDB data for all sites and parameters."""
        logging.info("🏔️ Starting HDB data load...")

        try:
            with connect_duckdb() as con:
                # Get all HDB sites with their site_datatype_ids
                hdb_sites = con.execute("""
                                        SELECT s.site_id, s.site_cd, s.site_nm, u.usbr_site_parameter_cd
                                        FROM site s
                                        INNER JOIN source src ON s.source_id = src.source_id
                                        INNER JOIN site_parameter sp ON s.site_id = sp.site_id
                                        INNER JOIN usbr_site_parameter u ON sp.site_parameter_id = u.site_parameter_id
                                        WHERE src.source_cd = 'HDB'
                                        ORDER BY s.site_cd
                                        """).fetchall() 
                
            logging.info(f"📍 Found {len(hdb_sites)} HDB site-parameter combinations to process")

            for site_id, site_code, site_name, usbr_site_parameter_code in hdb_sites:
                self._process_hdb_site(site_id, site_code, site_name, usbr_site_parameter_code)

        except Exception as e:
            logging.error(f"❌ Error in HDB data load: {e}")
            raise

    def _process_nwis_site(self, site_id: int, site_code: str, site_name: str):
        """Process a single NWIS site."""
        logging.info(f"🔄 Processing NWIS site: {site_code} - {site_name}")

        try:
            # Get parameters for the site
            parameter_codes = fetch_site_parameters(site_id)
            if not parameter_codes:
                logging.warning(f"⚠️ No parameters found for NWIS site {site_code}. Skipping.")
                return
            
            site_data = []

            for parameter_code in parameter_codes:
                logging.info(f"  📊 Fetching parameter {parameter_code}")

                try:
                    # Extract data
                    raw_data = self.nwis_extractor.fetch_timeseries_data(
                        site_code=site_code,
                        parameter_code=parameter_code,
                        start_date=self.start_date,
                        end_date=self.end_date
                    )

                    if raw_data is None or raw_data.empty:
                        logging.warning(f"⚠️ No data returned for site {site_code} and parameter {parameter_code}.")
                        continue

                    # Transform data
                    transformed_data = self.nwis_transformer.transform(
                        raw_data,
                        site_code=site_code,
                        parameter_code=parameter_code
                    )

                    if not transformed_data.empty:
                        site_data.append(transformed_data)
                        logging.info(f"    ✅ Got {len(transformed_data)} records for parameter {parameter_code}")

                except Exception as e:
                    logging.error(f"❌ Error processing parameter {parameter_code}: {e}")
                    continue
            
            # Load all data for the site
            if site_data:
                combined_data = pd.concat(site_data, ignore_index=True)
                self.loader.load_timeseries_data(combined_data, site_code)
                self.total_records += len(combined_data)
                logging.info(f"✅ Loaded {len(combined_data)} total records for site {site_code}")
            self.sites_processed += 1
        except Exception as e:
            logging.error(f"❌ Error processing NWIS site {site_code}: {e}")
            self.sites_failed += 1

    def _process_hdb_site(self, site_id: int, site_code: str, site_name: str, usbr_site_parameter_code: str):
        """Process a single HDB site-parameter combination."""
        logging.info(f"🔄 Processing HDB site: {site_code} - {site_name} ({usbr_site_parameter_code})")

        try:
            # Extract data
            raw_data = self.hdb_extractor.fetch_timeseries_data(
                usbr_site_parameter_code=usbr_site_parameter_code,
                start_date=self.start_date,
                end_date=self.end_date
            )

            if raw_data is None or raw_data.empty:
                logging.warning(f"⚠️ No data returned for HDB site {site_code} ({usbr_site_parameter_code}).")
                return
            
            # Transform data
            transformed_data = self.hdb_transformer.transform(
                raw_data, 
                site_code=site_code, 
                usbr_site_parameter_code=usbr_site_parameter_code
            )

            if not transformed_data.empty:
                self.loader.load_timeseries_data(transformed_data, site_code)
                self.total_records += len(transformed_data)
                logging.info(f"✅ Loaded {len(transformed_data)} records for HDB site {site_code} ({usbr_site_parameter_code})")

            self.sites_processed += 1

        except Exception as e:
            logging.error(f"❌ Error processing HDB site {site_code} ({usbr_site_parameter_code}): {e}")
            self.sites_failed += 1

    def generate_summary_report(self):
        """Generate and log a summary report of the initial load."""
        logging.info("📊 INITIAL LOAD SUMMARY REPORT")
        logging.info("=" * 50)
        logging.info(f"🏁 Load completed: {dt.datetime.now()}")
        logging.info(f"📅 Date range: {self.start_date} to {self.end_date}")
        logging.info(f"✅ Sites processed successfully: {self.sites_processed}")
        logging.info(f"❌ Sites failed: {self.sites_failed}")
        logging.info(f"📊 Total records loaded: {self.total_records:,}")

        # Export metadata for review
        try:
            write_meta_tables_to_csv()
            logging.info("📋 Metadata tables exported to artifacts/")
        except Exception as e:
            logging.error(f"❌ Failed to export metadata: {e}")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Initial historical data load for UCPO Water Data System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full historical load (default: 1900-01-01 to today)
    python scripts/initial_load.py

    # Load specific date range
    python scripts/initial_load.py --start-date 2020-01-01 --end-date 2023-12-31

    # Load recent data only
    python scripts/initial_load.py --start-date 2023-01-01
        """
    )

    parser.add_argument(
        '--start-date',
        type=str,
        default='1900-01-01',
        help='Start date for data load (YYYY-MM-DD format). Default: 1900-01-01'
    )

    parser.add_argument(
        '--end-date', 
        type=str,
        default=dt.date.today().strftime('%Y-%m-%d'),
        help='End date for data load (YYYY-MM-DD format). Default: today'
    )

    parser.add_argument(
        '--skip-nwis',
        action='store_true',
        help='Skip NWIS data loading'
    )

    parser.add_argument(
        '--skip-hdb',
        action='store_true', 
        help='Skip HDB data loading'
    )

    return parser.parse_args()


def main():
    """Main execution function."""
    print("🚀 UCPO Water Data System - Initial Load")
    print("=" * 50)

    # Parse arguments
    args = parse_arguments()

    # Setup logging
    setup_logging('initial_load', level=logging.INFO)

    # Log startup info
    logging.info("🚀 Starting initial data load process")
    logging.info(f"📅 Date range: {args.start_date} to {args.end_date}")

    try:
        # Initialize the loader
        loader = InitialDataLoader(args.start_date, args.end_date)

        # Initialize database
        loader.initialize_database()

        # Load data from different sources
        if not args.skip_nwis:
            loader.load_nwis_data()
        else:
            logging.info("⏭️ Skipping NWIS data load")

        if not args.skip_hdb:
            loader.load_hdb_data()
        else:
            logging.info("⏭️ Skipping HDB data load")

        # Generate summary report
        loader.generate_summary_report()

        print("\n🎉 Initial load completed successfully!")
        print("📊 Check logs for details: logs/initial_load_*.log")

    except KeyboardInterrupt:
        logging.warning("⚠️ Initial load interrupted by user")
        print("\n⚠️ Load process interrupted")
        sys.exit(1)

    except Exception as e:
        logging.error(f"💥 Fatal error in initial load: {e}")
        print(f"\n💥 Fatal error: {e}")
        print("📋 Check logs for details")
        sys.exit(1)


if __name__ == "__main__":
    main()
