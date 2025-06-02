
import logging
from typing import Optional
import pandas as pd
import dataretrieval.nwis as nwis


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
