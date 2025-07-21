from contextlib import contextmanager
from pathlib import Path
from typing import Union, List, Optional
import pandas as pd
import duckdb
import logging
import os
import inspect


try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional

# Default path to the Duckdb file (can be overridden)
ENV_DATA_PATH = os.getenv('DATA_STORAGE_PATH')
if ENV_DATA_PATH:
    DEFAULT_DB_PATH = Path(ENV_DATA_PATH) / 'hydrologic_data.duckdb'
    # DEFAULT_DL_PATH = Path(ENV_DATA_PATH) / 'hydrology_datalake'
else:
    DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / 'data' / 'hydrologic_data.duckdb'
    # DEFAULT_DL_PATH = Path(__file__).resolve().parents[1] / 'data' / 'hydrology_datalake'


@contextmanager
def connect_duckdb(path: Path = DEFAULT_DB_PATH):
    """
    Context manager to handle DuckDB connection.

    Args:
        path (Path): Path to the DuckDB database file. This is either set in the dot env file
                     or defaults to 'data/hydrologic_data.duckdb' in the project root.
    Yields:
        duckdb.DuckDBPyConnection: The active database connection.
    """
    path.parent.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
    logging.info(f"Connecting to Duckdb at {path}...")

    con = duckdb.connect(str(path))
    try:
        yield con
    except Exception as e:
        logging.error(f"Error connecting to Duckdb at {path}: {e}")
        raise

    finally:
        if 'con' in locals():
            con.close()


def execute_sql_script(
        sql_file: Union[str, Path],
        verbose: bool = False
        ) -> List[pd.DataFrame]:
    """
    Executes the SQL script.
    - Returns a list of DataFrames, one for each SELECT in the script.
    - Non-SELECT statements are executed but do not return a result.

    Parameters:
    sql_file (str or Path): Path to the .sql file
    duckdb_path (str or Path): Path to the DuckDB database file
    verbose (bool): If True, prints each statement as it's executed

    Returns:
        List[pd.DataFrame]: Results from all SELECT statements
    """
    sql_file = Path(sql_file)

    if not sql_file.exists():
        logging.error(f"SQL file {sql_file} does not exist.")
        raise FileNotFoundError(f"SQL file {sql_file} does not exist.")

    with open(sql_file, 'r') as file:
        sql_script = file.read()

    with connect_duckdb() as con:
        statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
        results = []

        for stmt in statements:
            if verbose:
                print(f"Executing:\n{stmt}\n")

            try:
                if stmt.lower().startswith('select'):
                    df = con.execute(stmt).fetchdf()
                    results.append(df)
                else:
                    con.execute(stmt)
            except Exception as e:
                logging.error(f"Error executing statement: {stmt}\n{e}")
                raise RuntimeError(f"Error executing statement: {stmt}\n{e}")

        con.commit()

    return results


def fetch_site_parameters(site_id: str) -> Optional[list]:
    """
    Fetch parameter codes for a given site from the duckdb database.
    Logs an error if the query fails or returns no results.
    """
    try:
        with connect_duckdb() as con:
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


def write_meta_tables_to_csv():
    """Write metadata tables to CSV files for inspection."""

    # Get the current working directory
    current_path = Path.cwd()

    # Try to get the file path if available
    try:
        frame = inspect.currentframe()
        if frame and frame.f_code.co_filename != '<stdin>':
            current_path = Path(frame.f_code.co_filename).resolve()
    except Exception:
        pass  # Fall back to cwd

    # Find project root by looking for marker directories
    # (e.g., 'src', 'data', etc.) in the current path.
    project_root = None
    search_paths = [current_path] + list(current_path.parents)

    for path in search_paths:
        if (path / 'src').exists() and (path / 'notebook').exists():
            project_root = path
            break

    if project_root is None:
        # Fallback: assume cwd is the project root
        if current_path.name == 'notebook':
            project_root = current_path.parent
        elif (current_path / 'src').exists():
            project_root = current_path
        else:
            project_root = current_path.parent

    artifacts_path = project_root / 'artifacts'

    meta_tables = ['site', 'parameter', 'site_parameter', 'usbr_site_parameter', 'source']
    for table in meta_tables:
        with connect_duckdb() as con:
            df = con.execute(f"SELECT * FROM {table}").df()
            output_file = artifacts_path / f"{table}.csv"
            df.to_csv(output_file, index=False)
            logging.info(f"✅ Wrote {table} to {output_file}")
