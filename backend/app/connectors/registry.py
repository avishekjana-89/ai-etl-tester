from app.connectors.base import BaseConnector
from app.connectors.sql_connector import SQLConnector
from app.connectors.file_connector import FileConnector


# Map connector type string → connector class
_REGISTRY: dict[str, type[BaseConnector]] = {
    "postgresql": SQLConnector,
    "mysql": SQLConnector,
    "mssql": SQLConnector,
    "csv": FileConnector,
    "excel": FileConnector,
    "parquet": FileConnector,
    "json": FileConnector,
}


def get_connector(connector_type: str) -> BaseConnector:
    """Factory: returns an unconnected connector instance."""
    cls = _REGISTRY.get(connector_type)
    if not cls:
        raise ValueError(
            f"Unknown connector type: '{connector_type}'. "
            f"Available: {list(_REGISTRY.keys())}"
        )
    return cls()


def create_connected_connector(connector_type: str, config: dict) -> BaseConnector:
    """Factory: returns a connected connector instance ready to use."""
    conn = get_connector(connector_type)
    conn.connect(config)
    return conn


def list_supported_types() -> list[str]:
    return list(_REGISTRY.keys())
