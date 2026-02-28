from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.connectors.base import BaseConnector


class FileConnector(BaseConnector):
    """
    CSV / Excel / Parquet / JSON connector via DuckDB.
    Supports single files or directories of files.
    """

    SUPPORTED_TYPES = {".csv", ".tsv", ".excel", ".parquet", ".json", ".xlsx", ".xls"}

    def __init__(self):
        self.conn: duckdb.DuckDBPyConnection | None = None
        self.tables: list[str] = []

    def connect(self, config: dict) -> None:
        file_path = Path(config["file_path"])
        file_type = config.get("file_type", "csv")
        
        self.conn = duckdb.connect()
        self.tables = []

        if file_path.is_dir():
            # Load all files in the directory
            for f in file_path.iterdir():
                if f.is_file() and f.suffix.lower() in self.SUPPORTED_TYPES:
                    table_name = f.name
                    self._register_file(f, table_name)
                    self.tables.append(table_name)
        else:
            # Single file loading
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            table_name = "data" # Default for backward compatibility if needed? 
            # Actually, let's use the file name as the primary name, but maybe keep 'data' as alias
            self._register_file(file_path, file_path.name)
            self.tables.append(file_path.name)
            
            # Alias to 'data' for backward compatibility
            self.conn.execute(f"CREATE VIEW data AS SELECT * FROM \"{file_path.name}\"")
            self.tables.append("data")

    def _register_file(self, path: Path, table_name: str) -> None:
        ext = path.suffix.lower()
        # Quote table name to handle dots and special chars
        quoted_name = f"\"{table_name}\""
        
        if ext in (".csv", ".tsv"):
            self.conn.execute(f"CREATE TABLE {quoted_name} AS SELECT * FROM read_csv_auto('{path}')")
        elif ext == ".parquet":
            self.conn.execute(f"CREATE TABLE {quoted_name} AS SELECT * FROM read_parquet('{path}')")
        elif ext == ".json":
            self.conn.execute(f"CREATE TABLE {quoted_name} AS SELECT * FROM read_json_auto('{path}')")
        elif ext in (".excel", ".xlsx", ".xls"):
            df = pd.read_excel(path)
            # Registering a DataFrame requires a name without dots for DuckDB's register()
            safe_alias = table_name.replace(".", "_").replace(" ", "_")
            self.conn.register(safe_alias, df)
            self.conn.execute(f"CREATE TABLE {quoted_name} AS SELECT * FROM {safe_alias}")

    def disconnect(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def test_connection(self) -> bool:
        try:
            if not self.tables:
                return False
            # Check the first table
            result = self.conn.execute(f"SELECT COUNT(*) FROM \"{self.tables[0]}\"").fetchone()
            return result is not None
        except Exception:
            return False

    def execute_query(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).df()

    def execute_scalar(self, query: str) -> Any:
        result = self.conn.execute(query).fetchone()
        return result[0] if result else None

    def get_tables(self) -> list[str]:
        return self.tables

    def get_checksum(self, query: str) -> str:
        """DuckDB implementation of checksum using hash aggregation."""
        try:
            # 1. Get column names
            sample = self.conn.execute(f"SELECT * FROM ({query}) AS t LIMIT 0").df()
            cols = list(sample.columns)
            if not cols: return "empty"

            # 2. Build hash aggregate
            # DuckDB's hash() returns a uint64
            concat_items = " || ".join([f"COALESCE(\"{c}\"::text, '')" for c in cols])
            checksum_sql = f"SELECT SUM(hash({concat_items})) FROM ({query}) AS t"
            
            result = self.conn.execute(checksum_sql).fetchone()
            return str(result[0]) if result and result[0] is not None else "0"
        except Exception:
            return "fallback-needed"

    def get_columns(self, table: str) -> list[dict]:
        # Quote table name correctly
        quoted_table = f"\"{table}\"" if "\"" not in table else table
        result = self.conn.execute(f"DESCRIBE {quoted_table}").df()
        return [
            {
                "name": row["column_name"],
                "type": row["column_type"],
                "nullable": True,
                "is_primary_key": False,
                "foreign_key": None,
                "is_unique": False,
            }
            for _, row in result.iterrows()
        ]

    def get_sample_data(self, table: str, limit: int = 5) -> list[dict]:
        """Fetch a few sample rows from the file-backed table."""
        try:
            quoted_table = f"\"{table}\"" if "\"" not in table else table
            df = self.conn.execute(f"SELECT * FROM {quoted_table} LIMIT {limit}").df()
            rows = []
            for _, row in df.iterrows():
                rows.append({col: str(val) for col, val in row.items()})
            return rows
        except Exception:
            return []
