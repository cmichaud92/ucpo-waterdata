
from collections import namedtuple
# from typing import List
import logging
from typing import Optional
import pandas as pd
import dataretrieval.nwis as nwis
import src.utils.duckdb_utils as db


def fetch_nwis_data(
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
        logging.error(f"Error fetching data for site {site_code} and parameter {parameter_code}: {e}")
        return None

    if df.empty:
        logging.warning(f"No data returned for site {site_code} and parameter {parameter_code}.")
        return None

    return df


def fetch_site_parameters(site_id: str) -> Optional[list]:
    """
    Fetch parameter codes for a given site from the duckdb database.
    Logs an error if the query fails or returns no results.
    """
    try:
        with db.connect_duckdb() as con:
            query = (
                "SELECT p.parameter_cd, "
                " FROM parameter p"
                " INNER JOIN site_parameter sp ON p.parameter_id = sp.parameter_id"
                f" WHERE sp.site_id = '{site_id}'"
            )
            params = con.execute(query).fetchall()
            if not params:
                logging.warning(f"No parameters found for site {site_id}.")
                return None
            return [param[0] for param in params]
    except Exception as e:
        logging.error(f"Error fetching parameters for site {site_id}: {e}")
        return None


def fetch_approval_status(site_cd: str) -> Optional[list]:
    # Create a named tuple for site information
    ApprovalStatus = namedtuple('ApprovalStatus', ['parameter_code', 'max_approval_date'])

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
