import duckdb
from pathlib import Path
from typing import Union, List
import pandas as pd
import logging


def execute_sql_script(
        sql_file: Union[str, Path],
        duckdb_path: Union[str, Path],
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
    duckdb_path = Path(duckdb_path)

    if not sql_file.exists():
        logging.error(f"SQL file {sql_file} does not exist.")
        raise FileNotFoundError(f"SQL file {sql_file} does not exist.")

    with open(sql_file, 'r') as file:
        sql_script = file.read()

    with duckdb.connect(duckdb_path) as conn:
        statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
        results = []

        for stmt in statements:
            if verbose:
                print(f"Executing:\n{stmt}\n")

            try:
                if stmt.lower().startswith('select'):
                    df = conn.execute(stmt).fetchdf()
                    results.append(df)
                else:
                    conn.execute(stmt)
            except Exception as e:
                logging.error(f"Error executing statement: {stmt}\n{e}")
                raise RuntimeError(f"Error executing statement: {stmt}\n{e}")

        conn.commit()

    return results
