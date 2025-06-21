
import logging
from typing import Optional
import pandas as pd
import dataretrieval.nwis as nwis


def fetch_nwis_data(
        site: str,
        pcode: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
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


def get_available_parameters(site: str, service_code: str = 'iv') -> list:
    """
    Query NWIS to get a list of available parameters for a given site and service type.
    """
    try:
        meta_df, meta_info = nwis.get_info(site=site)
    except Exception as e:
        logging.error(f"Error fetching metadata for site {site}: {e}")
        return []

    if meta_df.empty:
        logging.warning(f"No metadata found for site {site}.")
        return []

    # Filter for specified service code
    if 'param_cd' in meta_df.columns and 'data_type_cd' in meta_df.columns:
        return meta_df.loc[meta_df['data_type_cd'] == service_code, 'param_cd'].unique().tolist()
    else:
        return []
