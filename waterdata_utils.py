import dataretrieval.nwis as nwis
import pandas as pd
import duckdb
import logging
from typing import Optional
from pathlib import Path


def fetch_data(
        site: str,
        pcode: str,
        start_date: str,
        end_date: str,
        service_code: str = 'iv'
        ) -> Optional[pd.DataFrame]:
    """
    Fetch data from NWIS for a given site and parameter code.
    Logs an error if the request fails.
    """
    try:
        df = nwis.get_record(
            sites=site,
            service=service_code,
            start=start_date,
            end=end_date,
            parameterCd=pcode
        )
    except Exception as e:
        logging.error(f"Error fetching data for site {site} and parameter {pcode}: {e}")
        return None

    if df.empty:
        logging.warning(f"No data returned for site {site} and parameter {pcode}.")
        return None

    return df


def transform_data(df: pd.DataFrame, site: str, pcode: str) -> pd.DataFrame:
    """
    Transform raw NWIS 'iv' data into standardized long format.

    Parameters:
        df: Raw dataframe from nwis.get_record()
        site: USGS site number
        pcode: Parameter code (e.g., '00060' for discharge)
    Returns:
        A cleaned DataFrame with standard columns:
        ['site', 'datetime', 'parameter', 'value', 'approval_status', 'year']
    """

    # Set fail-safe defaults incase no data is available
    if df is None or df.empty:
        logging.warning(f"No data to transform for site {site} and parameter {pcode}.")
        return pd.DataFrame()

    # Datetime is the index in the raw data
    df = df.reset_index()

    # Ensure datetime column is present
    if 'datetime' not in df.columns:
        logging.error(f"Missing 'datetime' column in data for site {site} and parameter {pcode}.")
        raise ValueError("Missing 'datetime' column.")

    # Identify value column and approval status column
    value_cols = [
        col for col in df.columns if col not in ('site_no', 'datetime') and not col.endswith('cd')
        ]
    code_cols = [col for col in df.columns if col.endswith('cd')]

    if len(value_cols) != 1:
        logging.error(
            f"Expected exactly one value column for site {site} and parameter {pcode}. "
            f"Found: {value_cols}"
        )
        raise ValueError("Expected exactly one value column.")
    if len(code_cols) != 1:
        logging.error(
            f"Expected exactly one code column for site {site} and parameter {pcode}. "
            f"Found: {code_cols}"
        )
        raise ValueError("Expected exactly one code column.")

    value_col = value_cols[0]
    code_col = code_cols[0]

    # Construct clean output
    df_clean = pd.DataFrame({
        'site': site,
        'datetime': pd.to_datetime(df['datetime']),
        'parameter': pcode,
        'value': pd.to_numeric(df[value_col], errors='coerce'),
        'approval_status': df[code_col].str[0],  # Assuming first character is the status (P or A)
    })

    # Derived field for partitioning
    df_clean['year'] = df_clean['datetime'].dt.year

    # Drop bad rows (e.g., NaN values in 'site', 'value' or 'datetime')
    required_fields = ['site', 'datetime', 'value']
    df_clean = df_clean.dropna(subset=required_fields)

    return df_clean


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
