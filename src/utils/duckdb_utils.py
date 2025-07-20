from contextlib import contextmanager
from pathlib import Path
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
else:
    DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / 'data' / 'hydrologic_data.duckdb'


@contextmanager
def connect_duckdb(path: Path = DEFAULT_DB_PATH):
    """
    Context manager to handle DuckDB connection.

    Args:
        path (Path): Path to the DuckDB database file.
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


def run_sql_file(file_path: Path):
    """
    Run a SQL file against the DuckDB database.

    Args:
        file_path (Path): Path to the SQL file to execute.
    """
    with connect_duckdb() as con:
        try:
            with open(file_path, 'r') as f:
                sql_script = f.read()
            con.execute(sql_script)
            logging.info(f"✅ Successfully executed SQL file: {file_path}")
        except Exception as e:
            logging.error(f"❌ Error executing SQL file {file_path}: {e}")
            raise


def refresh_db_from_csv(table_name: str, csv_path: str):
    """
    Refresh a DuckDB table from a CSV file.
    """
    with connect_duckdb() as con:
        # Truncate the table
        con.execute(f"DELETE FROM {table_name}")
        # Fetch the csv data and insert it into the table
        con.register("csv_data", pd.read_csv(f"{csv_path}/{table_name}.csv",
                                             dtype={"site_cd": "string",
                                                    "parameter_cd": "string"}))
        con.execute(f"INSERT INTO {table_name} SELECT * FROM csv_data")


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
