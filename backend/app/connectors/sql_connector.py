from typing import Any

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from app.connectors.base import BaseConnector


class SQLConnector(BaseConnector):
    """
    Universal SQL connector via SQLAlchemy.
    Supports PostgreSQL out of the box. Easily extensible to
    MySQL, MSSQL, Oracle by adding URL templates.
    """

    URL_TEMPLATES = {
        "postgresql": "postgresql://{user}:{password}@{host}:{port}/{database}",
        "mysql": "mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        "mssql": "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
    }

    def __init__(self):
        self.engine: Engine | None = None
        self.db_type: str = ""

    def connect(self, config: dict) -> None:
        self.db_type = config.get("type", "postgresql")
        template = self.URL_TEMPLATES.get(self.db_type)
        if not template:
            raise ValueError(f"Unsupported database type: {self.db_type}")
        url = template.format(**config)
        self.engine = create_engine(url, pool_pre_ping=True)

    def disconnect(self) -> None:
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def execute_query(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as conn:
            return pd.read_sql(text(query), conn)

    def execute_scalar(self, query: str) -> Any:
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            row = result.fetchone()
            return row[0] if row else None

    def get_checksum(self, query: str) -> str:
        """
        Calculates a checksum of the result set at the database level.
        Wraps the user query and hashes the columns.
        """
        # We need to know the columns to hash them. 
        # A quick way to get columns without fetching data is LIMIT 0
        with self.engine.connect() as conn:
            # 1. Get column names
            sample = conn.execute(text(f"SELECT * FROM ({query}) as t LIMIT 0"))
            cols = sample.keys()
            
            if not cols:
                return "empty"

            # 2. Build hashing SQL
            # We concatenate all columns and take a hash. 
            # Dialect specific casting to text.
            if self.db_type == "mysql":
                concat_items = ", ".join([f"IFNULL(CAST(`{c}` AS CHAR), '')" for c in cols])
                hash_expr = f"MD5(CONCAT({concat_items}))"
                # Use BIT_XOR for non-deterministic row order aggregate if possible, 
                # but SUM(CRC32) or similar is easier. 
                # For simplicity and cross-db safety, let's hash each row then SUM the hash (as numeric if possible)
                # Actually, let's just use a simple string aggregation for now, or fetch just the hashes.
                # BETTER: Fetch a single row that is the XOR sum of hashes.
                checksum_sql = f"SELECT BIT_XOR(CONV(LEFT(MD5(CONCAT({concat_items})), 16), 16, 10)) FROM ({query}) as t"
            else: # postgresql / duckdb
                concat_items = " || ".join([f"COALESCE(\"{c}\"::text, '')" for c in cols])
                # In PG, we can aggregate hashes.
                # We use a custom aggregate or just SUM(HASHTEXT(concat))
                # For simplicity:
                checksum_sql = f"SELECT SUM(hashtext({concat_items})) FROM ({query}) as t"

            # 3. Execute
            try:
                result = conn.execute(text(checksum_sql)).fetchone()
                return str(result[0]) if result and result[0] is not None else "0"
            except Exception:
                # Fallback: if complex aggregation fails, return a dummy that forces full comparison
                return "fallback-needed"

    # Schemas to skip — these are internal system schemas
    SKIP_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast", "mysql", "performance_schema", "sys"}

    def get_schemas(self) -> list[str]:
        """Return all user-facing schemas in the database."""
        insp = inspect(self.engine)
        return [s for s in insp.get_schema_names() if s not in self.SKIP_SCHEMAS]

    def get_tables(self) -> list[str]:
        """Return schema-qualified table names across ALL schemas."""
        insp = inspect(self.engine)
        tables = []
        for schema in self.get_schemas():
            for table in insp.get_table_names(schema=schema):
                tables.append(f"{schema}.{table}")
        return tables

    def get_columns(self, table: str) -> list[dict]:
        """
        Return full column metadata including PK, FK, unique constraints.
        Each column dict contains: name, type, nullable, is_primary_key,
        foreign_key (ref table.column or null), is_unique.
        """
        insp = inspect(self.engine)
        if "." in table:
            schema, table_name = table.split(".", 1)
        else:
            schema, table_name = None, table

        # Get basic column info
        columns = insp.get_columns(table_name, schema=schema)

        # Get primary key columns
        try:
            pk_info = insp.get_pk_constraint(table_name, schema=schema)
            pk_columns = set(pk_info.get("constrained_columns", []))
        except Exception:
            pk_columns = set()

        # Get foreign key info: column → "ref_table.ref_column"
        fk_map = {}
        try:
            fk_list = insp.get_foreign_keys(table_name, schema=schema)
            for fk in fk_list:
                ref_table = fk.get("referred_table", "")
                ref_schema = fk.get("referred_schema", "")
                ref_columns = fk.get("referred_columns", [])
                constrained_columns = fk.get("constrained_columns", [])
                for local_col, ref_col in zip(constrained_columns, ref_columns):
                    ref_full = f"{ref_schema}.{ref_table}.{ref_col}" if ref_schema else f"{ref_table}.{ref_col}"
                    fk_map[local_col] = ref_full
        except Exception:
            pass

        # Get unique constraint columns
        unique_columns = set()
        try:
            unique_list = insp.get_unique_constraints(table_name, schema=schema)
            for uc in unique_list:
                for col in uc.get("column_names", []):
                    unique_columns.add(col)
        except Exception:
            pass

        return [
            {
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "is_primary_key": col["name"] in pk_columns,
                "foreign_key": fk_map.get(col["name"]),
                "is_unique": col["name"] in unique_columns or col["name"] in pk_columns,
            }
            for col in columns
        ]

    def get_sample_data(self, table: str, limit: int = 5) -> list[dict]:
        """Fetch a few sample rows from a table as list of dicts."""
        try:
            query = f"SELECT * FROM {table} LIMIT {limit}"
            df = self.execute_query(query)
            rows = []
            for _, row in df.iterrows():
                rows.append({col: str(val) for col, val in row.items()})
            return rows
        except Exception:
            return []
