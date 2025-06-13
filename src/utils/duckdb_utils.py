from contextlib import contextmanager
from pathlib import Path
import duckdb
import logging
import os

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
