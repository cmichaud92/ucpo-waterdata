import pandas as pd
import logging
from pathlib import Path
import duckdb


def write_to_datalake(df: pd.DataFrame, site: str, output_root: str) -> None:
    """
    Write transformed USGS IV data to partitioned parquet files in the datalake.

    Parameters:
        df              : Transformed DataFrame with columns including
                          ['site', 'datetime', 'parameter', 'value', 'approval_status', 'year']
        site            : USGS site number
        output_root     : Root directory for the datalake
    """
    if df.empty:
        logging.warning(f"No data to write for site {site}.")
        return

    # Sort data by datetime for performance and compression
    df_sorted = df.sort_values(by='datetime')

    # Partition by year and site and write to parquet
    for year, group in df_sorted.groupby('year'):
        output_path = Path(output_root) / "timeseries_iv" / f"site={site}" / f"year={year}"
        output_path.mkdir(parents=True, exist_ok=True)
        file_path = output_path / "data.parquet"
        try:
            group = group.copy()

            # Strip timezone from datetime if present
            if isinstance(group["datetime"].dtype, pd.DatetimeTZDtype):
                group["datetime"] = group["datetime"].dt.tz_localize(None)

            # Ensure correct data types
            group = group.astype({
                "site": "string",
                "datetime": "datetime64[ns]",
                "parameter": "string",
                "value": "float64",
                "approval_status": "string",
                "year": "int64"
            })

            duckdb.register("temp_df", group)
            duckdb.sql(f"COPY temp_df TO '{file_path}' (FORMAT PARQUET)")
            duckdb.unregister("temp_df")
            logging.info(f"{len(group)} rows → {file_path}")
        except Exception as e:
            logging.error(f"Error writing data for site {site} and year {year}: {e}")
            raise
