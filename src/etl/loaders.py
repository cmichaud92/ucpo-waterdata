import pandas as pd
import os
import logging
from pathlib import Path
import duckdb
from typing import Union

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional

# Default path to the data lake files (can be overridden)
ENV_DATA_PATH = os.getenv('DATA_STORAGE_PATH')
if ENV_DATA_PATH:
    DEFAULT_DATALAKE_PATH = Path(ENV_DATA_PATH) / 'hydrology_datalake'
else:
    DEFAULT_DATALAKE_PATH = Path(__file__).resolve().parents[1] / 'data' / 'hydrology_datalake'


def DataLakeLoader(
        df: pd.DataFrame,
        site_code: str,
        datalake_root: Union[str, Path] = DEFAULT_DATALAKE_PATH
        ) -> None:
    """
    Write transformed USGS IV data to partitioned parquet files in the datalake.

    Parameters:
        df              : Transformed DataFrame with columns including
                          ['site', 'datetime', 'parameter', 'value', 'approval_status', 'year']
        site            : USGS site number
        datalake_root   : Root directory for the datalake
    """
    if df.empty:
        logging.warning(f"No data to write for site {site_code}.")
        return

    # Sort data by datetime for performance and compression
    df_sorted = df.sort_values(by='read_ts')

    # Partition by year and site and write to parquet
    for year, group in df_sorted.groupby('year'):
        datalake_path = Path(datalake_root) / "timeseries_iv" / f"site={site_code}" / f"year={year}"
        datalake_path.mkdir(parents=True, exist_ok=True)
        file_path = datalake_path / "data.parquet"
        try:
            group = group.copy()

            # Strip timezone from datetime if present
            if isinstance(group["read_ts"].dtype, pd.DatetimeTZDtype):
                group["read_ts"] = group["read_ts"].dt.tz_localize(None)

            # Ensure correct data types
            group = group.astype({
                "site_cd": "string",
                "read_ts": "datetime64[ns]",
                "parameter_cd": "string",
                "value": "float64",
                "approval_status": "string",
                "year": "int64"
            })

            duckdb.register("temp_df", group)
            duckdb.sql(f"COPY temp_df TO '{file_path}' (FORMAT PARQUET)")
            duckdb.unregister("temp_df")
            logging.info(f"{len(group)} rows → {file_path}")
        except Exception as e:
            logging.error(f"Error writing data for site {site_code} and year {year}: {e}")
            raise
