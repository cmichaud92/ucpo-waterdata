import src.utils.duckdb_utils as db
import src.utils.fetch_data as fd
import pandas as pd
import logging
import datetime as dt
from src.utils.transform_data import transform_nwis_iv_data
from src.utils.write_to_datalake import write_to_datalake


def process_nwis_iv_data_by_date(
        start_date: str = '1990-01-01',
        end_date: str = dt.date.today().strftime('%Y-%m-%d'),
        mode: str = 'initial'):
    """
    Process NWIS data for given date range.

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        mode (str): 'initial' or 'update'
    """
    logging.info(f"Processing NWIS IV data from {start_date} to {end_date} in {mode} mode.")

    # Fetch list of NWIS sites from DuckDB
    with db.connect_duckdb() as con:
        site_info = con.execute("SELECT site_id, site_cd FROM site WHERE source_id = 1;").fetchall()

    logging.info(f"Fetched {len(site_info)} site codes from DuckDB.")

    # Loop through sites and parameters and fetch data
    for site_id, site_code in site_info:
        logging.info(f"Fetching data for site {site_code}")
        all_data = []
        parameter_codes = fd.fetch_site_parameters(site_id)
        if not parameter_codes:
            logging.warning(
                f"No parameters found for site {site_code} (site_id={site_id}). Skipping."
                )
            continue

        for parameter_code in parameter_codes:
            logging.info(f"  Fetching parameter {parameter_code}")
            try:
                df = fd.fetch_nwis_data(site_code, parameter_code, start_date, end_date)
                if df is None or df.empty:
                    logging.warning(
                        f"No data found for site {site_code} with parameter {parameter_code}."
                        )
                    continue
                df = transform_nwis_iv_data(df, site_code, parameter_code)
            except Exception as e:
                logging.error(
                    f"Error processing site {site_code} and parameter {parameter_code}: {e}"
                    )
                continue
            if df is not None and not df.empty:
                all_data.append(df)
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            write_to_datalake(df_combined, site_code)
            logging.info(f"Combined data for site {site_code} successfully written to datalake.")


def update_nwis_iv_data():
    """
    fetch and process an update for NWIS iv data.
    """
    logging.info("Updating NWIS IV data lake tables.")

    # Fetch list of NWIS sites from DuckDB
    with db.connect_duckdb() as con:
        site_info = con.execute("SELECT site_id, site_cd FROM site WHERE source_id = 1;").fetchall()

    logging.info(f"Fetched {len(site_info)} site codes from DuckDB.")

    # Loop through sites and parameters and fetch data
    for site_id, site_code in site_info:
        logging.info(f"Fetching data for site {site_code}")
        all_data = []
        parameter_codes = fd.fetch_approval_status(site_id)
        if not parameter_codes:
            logging.warning(
                f"No parameters found for site {site_code} (site_id={site_id}). Skipping."
                )
            continue

        for parameter_code, max_approval_date in parameter_codes:
            logging.info(f"  Fetching parameter {parameter_code}")
            try:
                df = fd.fetch_nwis_data(site_code, 
                                        parameter_code, 
                                        max_approval_date, 
                                        dt.date.today().strftime('%Y-%m-%d'))
                if df is None or df.empty:
                    logging.warning(
                        f"No data found for site {site_code} with parameter {parameter_code}."
                        )
                    continue
                df = transform_nwis_iv_data(df, site_code, parameter_code)
            except Exception as e:
                logging.error(
                    f"Error processing site {site_code} and parameter {parameter_code}: {e}"
                    )
                continue
            if df is not None and not df.empty:
                all_data.append(df)
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            write_to_datalake(df_combined, site_code)
            logging.info(f"Combined data for site {site_code} successfully written to datalake.")
