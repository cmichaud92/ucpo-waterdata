
from collections import namedtuple
# from typing import List
import logging
from typing import Optional
import pandas as pd
import dataretrieval.nwis as nwis
import src.database.connection as db


def NWISExtractor(
        site_code: str,
        parameter_code: Optional[str] = None,
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
            sites=site_code,
            service=service_code,
            start=start_date,
            end=end_date,
            parameterCd=parameter_code
        )
    except Exception as e:
        logging.error(
            f"Error fetching data for site {site_code} and parameter {parameter_code}: {e}"
            )
        return None

    if df.empty:
        logging.warning(
            f"No data returned for site {site_code} and parameter {parameter_code}."
        )
        return None

    return df


# Figure out the best integration for update dates
def fetch_approval_status(site_cd: str) -> Optional[list]:
    # Create a named tuple for site information
    ApprovalStatus = namedtuple(
        'ApprovalStatus', ['parameter_code', 'max_approval_date']
        )

    query = (
        "SELECT parameter_cd, strftime(max(datetime_utc), '%Y-%m-%d') AS date"
        " FROM vw_nwis_iv_local"
        f" WHERE site_cd = '{site_cd}'"
        " AND approval_status = 'A'"
        " GROUP BY site_cd, parameter_cd"
    )

    with db.connect_duckdb() as con:
        result = con.execute(query).fetchall()
    if result:
        # Return a list of ApprovalStatus namedtuples for all results
        return [ApprovalStatus(approval[0], approval[1]) for approval in result]
    else:
        return None
