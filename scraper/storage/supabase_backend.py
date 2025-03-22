import logging
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from supabase import create_client, Client

from scraper.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class SupabaseBackend(StorageBackend):
    """
    Supabase storage backend implementation.

    This backend stores data in Supabase tables using the Supabase API.
    """

    def __init__(
        self,
        url: str,
        key: str,
        finn_codes_table: str = "finn_codes",
        properties_table: str = "properties",
    ):
        """
        Initialize the Supabase backend with connection details.

        Args:
            url: Supabase project URL
            key: Supabase API key
            finn_codes_table: Name of the table to store finn codes
            properties_table: Name of the table to store properties
        """
        self.url = url
        self.key = key
        self.finn_codes_table = finn_codes_table
        self.properties_table = properties_table
        self.client = None

    def initialize(self) -> None:
        """Initialize the Supabase connection and ensure tables exist."""
        try:
            self.client = create_client(self.url, self.key)
            logger.info(f"Connected to Supabase at {self.url}")

            # Check if tables exist by querying them
            # Since Supabase doesn't have a direct way to check if tables exist,
            # we'll try to query them and handle any errors
            try:
                self.client.table(self.finn_codes_table).select(
                    "count", count="exact"
                ).limit(1).execute()
                logger.info(f"Table '{self.finn_codes_table}' exists in Supabase")
            except Exception as e:
                logger.error(f"Error accessing table '{self.finn_codes_table}': {e}")
                logger.info(
                    f"You might need to create the table '{self.finn_codes_table}' in Supabase with columns: finn_code (primary key), fetched_at, scrape_status"
                )

            try:
                self.client.table(self.properties_table).select(
                    "count", count="exact"
                ).limit(1).execute()
                logger.info(f"Table '{self.properties_table}' exists in Supabase")
            except Exception as e:
                logger.error(f"Error accessing table '{self.properties_table}': {e}")
                logger.info(
                    f"You might need to create the table '{self.properties_table}' in Supabase with appropriate columns"
                )

        except Exception as e:
            logger.error(f"Error connecting to Supabase: {e}")
            raise

    def save_finn_codes(self, finn_codes_data: List[Dict[str, Any]]) -> None:
        """Save finn codes to Supabase."""
        if self.client is None:
            self.initialize()

        try:
            # Insert data with upsert (update on conflict)
            data_to_insert = []
            for item in finn_codes_data:
                data_item = {
                    "finn_code": item["finn_code"],
                    "fetched_at": item["fetched_at"],
                    "scrape_status": item.get("scrape_status", "pending"),
                    "listing_status": item.get("listing_status", "active"),
                    "last_date_checked": item.get(
                        "last_date_checked", item["fetched_at"]
                    ),
                }
                data_to_insert.append(data_item)

            # Split into chunks to avoid request size limits
            chunk_size = 100
            for i in range(0, len(data_to_insert), chunk_size):
                chunk = data_to_insert[i : i + chunk_size]
                self.client.table(self.finn_codes_table).upsert(chunk).execute()

            logger.info(f"Saved {len(finn_codes_data)} finn codes to Supabase")
        except Exception as e:
            logger.error(f"Error saving finn codes to Supabase: {e}")

    def fetch_finn_codes(self, select_all: bool = False) -> List[tuple]:
        """Fetch finn codes from Supabase."""
        if self.client is None:
            self.initialize()

        try:
            query = self.client.table(self.finn_codes_table).select("finn_code")

            if not select_all:
                query = query.eq("scrape_status", "pending")

            response = query.execute()

            # Format as list of tuples to match the SQLite backend format
            finn_codes = [(item["finn_code"],) for item in response.data]

            logger.info(f"Fetched {len(finn_codes)} finn codes from Supabase")
            return finn_codes
        except Exception as e:
            logger.error(f"Error fetching finn codes from Supabase: {e}")
            return []

    def save_property_data(self, property_data: Dict[str, Any]) -> None:
        """Save property data to Supabase."""
        if self.client is None:
            self.initialize()

        try:
            # Ensure scrape_status is set if not provided
            if "scrape_status" not in property_data:
                property_data["scrape_status"] = "success"

            # Upsert the property data
            self.client.table(self.properties_table).upsert(property_data).execute()

            logger.info(
                f"Saved property data for finn code {property_data.get('finn_code')} to Supabase"
            )
        except Exception as e:
            logger.error(f"Error saving property data to Supabase: {e}")

    def update_finn_code_status(self, finn_code: str, status: str) -> None:
        """Update the scrape status of a finn code in Supabase."""
        if self.client is None:
            self.initialize()

        try:
            self.client.table(self.finn_codes_table).update(
                {"scrape_status": status}
            ).eq("finn_code", finn_code).execute()
            logger.info(f"Updated scrape status for finn code {finn_code} to {status}")
        except Exception as e:
            logger.error(f"Error updating finn code status in Supabase: {e}")

    def export_to_csv(self, csv_name: str) -> None:
        """Export properties from Supabase to a CSV file."""
        if self.client is None:
            self.initialize()

        try:
            # Fetch all properties
            response = self.client.table(self.properties_table).select("*").execute()

            # Convert to DataFrame and save to CSV
            df = pd.DataFrame(response.data)
            df.to_csv(csv_name, index=False)

            logger.info(f"Exported properties to {csv_name}")
        except Exception as e:
            logger.error(f"Error exporting properties to {csv_name}: {e}")

    def export_finn_codes_to_csv(self, csv_name: str) -> None:
        """Export finn codes from Supabase to a CSV file."""
        if self.client is None:
            self.initialize()

        try:
            # Fetch all finn codes
            response = self.client.table(self.finn_codes_table).select("*").execute()

            # Convert to DataFrame and save to CSV
            df = pd.DataFrame(response.data)
            df.to_csv(csv_name, index=False)

            logger.info(f"Exported finn codes to {csv_name}")
        except Exception as e:
            logger.error(f"Error exporting finn codes to {csv_name}: {e}")

    def close(self) -> None:
        """Close the Supabase connection."""
        # Supabase client doesn't require explicit closing
        self.client = None
        logger.info("Supabase connection closed")

    def update_finn_code_status(self, finn_code: str, status: str) -> None:
        """Update the scrape status of a finn code in Supabase."""
        if self.client is None:
            self.initialize()

        try:
            self.client.table(self.finn_codes_table).update(
                {"scrape_status": status}
            ).eq("finn_code", finn_code).execute()
            logger.info(f"Updated scrape status for finn code {finn_code} to {status}")
        except Exception as e:
            logger.error(f"Error updating finn code status in Supabase: {e}")

    def update_finn_code_status_fields(self, finn_code: str, **kwargs) -> None:
        """Updates multiple status fields of a finn_code in Supabase."""
        if self.client is None:
            self.initialize()

        if not kwargs:
            return

        try:
            self.client.table(self.finn_codes_table).update(kwargs).eq(
                "finn_code", finn_code
            ).execute()
            logger.info(f"Updated status fields for Finn code {finn_code}: {kwargs}")
        except Exception as e:
            logger.error(f"Error updating finn code status fields in Supabase: {e}")

    def fetch_finn_codes_with_status(self, status: str = None) -> List[tuple]:
        """
        Fetches Finn codes with a specific listing status.

        Args:
            status: Listing status to filter by (e.g., 'active', 'inactive')

        Returns:
            List of (finn_code, last_date_checked) tuples
        """
        if self.client is None:
            self.initialize()

        try:
            query = self.client.table(self.finn_codes_table).select(
                "finn_code", "last_date_checked"
            )

            if status:
                query = query.eq("listing_status", status)

            response = query.execute()

            # Format as list of tuples
            result = [
                (item["finn_code"], item["last_date_checked"]) for item in response.data
            ]

            logger.info(
                f"Fetched {len(result)} finn codes with status {status or 'any'} from Supabase"
            )
            return result
        except Exception as e:
            logger.error(
                f"Error fetching finn codes with status {status} from Supabase: {e}"
            )
            return []
