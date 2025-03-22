from typing import Dict, Any
import logging

from scraper.storage.base import StorageBackend
from scraper.storage.sqlite_backend import SQLiteBackend
from scraper.storage.csv_backend import CSVBackend
from scraper.storage.supabase_backend import SupabaseBackend

logger = logging.getLogger(__name__)


def create_storage_backend(config: Dict[str, Any]) -> StorageBackend:
    """
    Create a storage backend based on the configuration.

    Args:
        config: Configuration dictionary

    Returns:
        A StorageBackend instance

    Raises:
        ValueError: If the backend type is not supported
    """
    backend_type = config.get("backend", "sqlite")

    if backend_type == "sqlite":
        db_name = config.get("sqlite", {}).get(
            "database_name", config.get("database_name", "finn_properties.db")
        )
        return SQLiteBackend(db_name)

    elif backend_type == "csv":
        finn_codes_path = config.get("csv", {}).get(
            "finn_codes_path", "data/finn_codes.csv"
        )
        properties_path = config.get("csv", {}).get(
            "properties_path", "data/properties.csv"
        )
        return CSVBackend(finn_codes_path, properties_path)

    elif backend_type == "supabase":
        supabase_config = config.get("supabase", {})
        url = supabase_config.get("url")
        key = supabase_config.get("key")

        if not url or not key:
            logger.error("Supabase URL and key must be provided in the configuration")
            raise ValueError(
                "Supabase URL and key must be provided in the configuration"
            )

        finn_codes_table = supabase_config.get("finn_codes_table", "finn_codes")
        properties_table = supabase_config.get("properties_table", "properties")

        return SupabaseBackend(url, key, finn_codes_table, properties_table)

    else:
        logger.error(f"Unsupported backend type: {backend_type}")
        raise ValueError(f"Unsupported backend type: {backend_type}")
