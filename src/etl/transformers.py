import pandas as pd
import logging


def NWISTransformer(df: pd.DataFrame, site_code: str, parameter_code: str) -> pd.DataFrame:
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
        logging.warning(
            f"No data to transform for site {site_code} and parameter {parameter_code}."
            )
        return pd.DataFrame()

    # Datetime is the index in the raw data
    df = df.reset_index()

    # Ensure datetime column is present
    if 'datetime' not in df.columns:
        logging.error(
            f"Missing 'datetime' column in data for site {site_code}"
            f" and parameter {parameter_code}."
        )
        raise ValueError("Missing 'datetime' column.")

    # Identify value column and approval status column
    value_cols = [
        col for col in df.columns if col not in ('site_no', 'datetime') and not col.endswith('cd')
        ]
    code_cols = [col for col in df.columns if col.endswith('cd')]

    if len(value_cols) != 1:
        logging.error(
            f"Expected exactly one value column for site {site_code}"
            f" and parameter {parameter_code}."
            f" Found: {value_cols}"
        )
        raise ValueError("Expected exactly one value column.")
    if len(code_cols) != 1:
        logging.error(
            f"Expected exactly one code column for site {site_code}"
            f" and parameter {parameter_code}."
            f" Found: {code_cols}"
        )
        raise ValueError("Expected exactly one code column.")

    value_col = value_cols[0]
    code_col = code_cols[0]

    # Construct clean output
    df_clean = pd.DataFrame({
        'site_cd': site_code,
        'read_ts': pd.to_datetime(df['datetime']),
        'parameter_cd': parameter_code,
        'value': pd.to_numeric(df[value_col], errors='coerce'),
        'approval_status': df[code_col].str[0],  # Assuming first character is the status (P or A)
    })

    # Derived field for partitioning
    df_clean['year'] = df_clean['read_ts'].dt.year

    # Drop bad rows (e.g., NaN values in 'site', 'value' or 'datetime')
    required_fields = ['site_cd', 'read_ts', 'parameter_cd', 'value']
    df_clean = df_clean.dropna(subset=required_fields)

    return df_clean
