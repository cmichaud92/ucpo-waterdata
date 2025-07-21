import database.connection as db
import src.utils.fetch_data as fd
import pandas as pd
import logging
import os
import datetime as dt

from etl.transformers import transform_nwis_iv_data
from src.utils.write_to_datalake import write_to_datalake
# from collections import namedtuple


def main():

    # Configure logging ------------------------------------------------
    os.makedirs('logs', exist_ok=True)
    log_name = 'logs/' + dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
    logging.basicConfig(filename=log_name,
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    # -------------------------------------------------------------------

    # Configure constants
    start_date = "1900-01-01"
    end_date = dt.date.today().strftime('%Y-%m-%d')  # Use today's date as end date

    logging.info(f"Processing data from {start_date} to {end_date}")

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


if __name__ == "__main__":
    main()
