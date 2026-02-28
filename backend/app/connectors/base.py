from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class BaseConnector(ABC):
    """
    Abstract base for all data source connectors.
    Every connector must return pandas DataFrames — this unifies
    SQL databases, files, and NoSQL sources for the test executor.
    """

    @abstractmethod
    def connect(self, config: dict) -> None:
        """Establish connection using the provided config."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Clean up connection resources."""
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        """Return True if the connection is healthy."""
        ...

    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as a DataFrame."""
        ...

    @abstractmethod
    def execute_scalar(self, query: str) -> Any:
        """Execute a query and return a single scalar value."""
        ...

    @abstractmethod
    def get_tables(self) -> list[str]:
        """Return list of table/collection names."""
        ...

    @abstractmethod
    def get_columns(self, table: str) -> list[dict]:
        """
        Return full column metadata:
        [{name, type, nullable, is_primary_key, foreign_key, is_unique}, ...]
        """
        ...

    @abstractmethod
    def get_sample_data(self, table: str, limit: int = 5) -> list[dict]:
        """Return a few sample rows from a table as list of dicts."""
        ...
    @abstractmethod
    def get_checksum(self, query: str) -> str:
        """
        Execute a query and return a single checksum/hash representing the entire 
        result set. Used for high-performance memory-safe comparisons.
        """
        ...
