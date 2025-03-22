import logging
from typing import List, Dict, Any, Optional, Union
import datetime
import pandas as pd
import numpy as np
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
        """Save property data to Supabase with proper type conversions."""
        if self.client is None:
            self.initialize()

        try:
            # Create a copy to avoid modifying original
            data = property_data.copy()

            # Ensure scrape_status is set if not provided
            if "scrape_status" not in data:
                data["scrape_status"] = "success"

            # Convert numeric fields
            numeric_fields = [
                "asking_price",
                "total_price",
                "costs",
                "joint_debt",
                "monthly_fee",
                "bedrooms",
                "internal_area",
                "usable_area",
                "external_usable_area",
                "floor",
                "build_year",
                "rooms",
            ]

            for field in numeric_fields:
                if field in data:
                    # Convert np.nan to None (will become NULL in database)
                    if isinstance(data[field], float) and np.isnan(data[field]):
                        data[field] = None
                    elif data[field] is not None:
                        # Try to convert string to numeric if it's a string
                        if isinstance(data[field], str):
                            try:
                                data[field] = float(data[field])
                            except ValueError:
                                # Keep as string if conversion fails
                                pass

            # Handle latitude/longitude
            if "latitude" in data and data["latitude"] is not None:
                try:
                    data["latitude"] = float(data["latitude"])
                except (ValueError, TypeError):
                    data["latitude"] = None

            if "longitude" in data and data["longitude"] is not None:
                try:
                    data["longitude"] = float(data["longitude"])
                except (ValueError, TypeError):
                    data["longitude"] = None

            # Convert timestamp fields
            timestamp_fields = ["last_date_checked"]
            for field in timestamp_fields:
                if field in data and data[field] is not None:
                    # Ensure it's in ISO format for PostgreSQL
                    if isinstance(data[field], datetime.datetime):
                        data[field] = data[field].isoformat()

            # Upsert the property data
            self.client.table(self.properties_table).upsert(data).execute()

            logger.info(
                f"Saved property data for finn code {data.get('finn_code')} to Supabase"
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
        """Updates multiple status fields of a finn_code in Supabase with type conversion."""
        if self.client is None:
            self.initialize()

        if not kwargs:
            return

        try:
            # Convert timestamp fields if present
            data = kwargs.copy()
            if "last_date_checked" in data and data["last_date_checked"] is not None:
                if isinstance(data["last_date_checked"], datetime.datetime):
                    data["last_date_checked"] = data["last_date_checked"].isoformat()

            self.client.table(self.finn_codes_table).update(data).eq(
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

    def property_exists(self, finn_code: str) -> bool:
        """Check if a property exists in the properties table."""
        if self.client is None:
            self.initialize()

        try:
            response = (
                self.client.table(self.properties_table)
                .select("finn_code")
                .eq("finn_code", finn_code)
                .execute()
            )
            return len(response.data) > 0
        except Exception as e:
            logger.error(f"Error checking property existence in Supabase: {e}")
            return False

    def fetch_properties(
        self, finn_codes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch properties from Supabase, optionally filtered by finn codes."""
        if self.client is None:
            self.initialize()

        try:
            query = self.client.table(self.properties_table).select("*")

            if finn_codes:
                # Supabase syntax for IN clause uses .in() method
                query = query.in_("finn_code", finn_codes)

            response = query.execute()

            logger.info(f"Fetched {len(response.data)} properties from Supabase")
            return response.data
        except Exception as e:
            logger.error(f"Error fetching properties from Supabase: {e}")
            return []
