import os
import csv
import logging
import datetime
from typing import List, Dict, Any, Optional, Union
import pandas as pd

from scraper.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class CSVBackend(StorageBackend):
    """
    CSV storage backend implementation.

    This backend stores data in CSV files instead of a database.
    It uses Pandas for data manipulation.
    """

    def __init__(self, finn_codes_path: str, properties_path: str):
        """
        Initialize the CSV backend with file paths.

        Args:
            finn_codes_path: Path to the finn codes CSV file
            properties_path: Path to the properties CSV file
        """
        self.finn_codes_path = finn_codes_path
        self.properties_path = properties_path
        self.finn_codes_df = None
        self.properties_df = None

    def initialize(self) -> None:
        """Initialize the CSV files if they don't exist."""
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.finn_codes_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.properties_path), exist_ok=True)

        # Initialize or load existing DataFrames
        if os.path.exists(self.finn_codes_path):
            try:
                self.finn_codes_df = pd.read_csv(self.finn_codes_path)
                logger.info(f"Loaded existing finn codes from {self.finn_codes_path}")
                # Add new columns if they don't exist
                if "listing_status" not in self.finn_codes_df.columns:
                    self.finn_codes_df["listing_status"] = "active"
                if "last_date_checked" not in self.finn_codes_df.columns:
                    self.finn_codes_df["last_date_checked"] = self.finn_codes_df[
                        "fetched_at"
                    ]
            except Exception as e:
                logger.error(
                    f"Error loading finn codes from {self.finn_codes_path}: {e}"
                )
                self.finn_codes_df = pd.DataFrame(
                    columns=[
                        "finn_code",
                        "fetched_at",
                        "scrape_status",
                        "listing_status",
                        "last_date_checked",
                    ]
                )
        else:
            logger.info(f"Creating new finn codes CSV at {self.finn_codes_path}")
            self.finn_codes_df = pd.DataFrame(
                columns=[
                    "finn_code",
                    "fetched_at",
                    "scrape_status",
                    "listing_status",
                    "last_date_checked",
                ]
            )
            self.finn_codes_df.to_csv(self.finn_codes_path, index=False)
        if os.path.exists(self.properties_path):
            try:
                self.properties_df = pd.read_csv(self.properties_path)
                logger.info(f"Loaded existing properties from {self.properties_path}")
            except Exception as e:
                logger.error(
                    f"Error loading properties from {self.properties_path}: {e}"
                )
                self._create_empty_properties_df()
        else:
            logger.info(f"Creating new properties CSV at {self.properties_path}")
            self._create_empty_properties_df()
            self.properties_df.to_csv(self.properties_path, index=False)

    def _create_empty_properties_df(self):
        """Create an empty properties DataFrame with all required columns."""
        self.properties_df = pd.DataFrame(
            columns=[
                "finn_code",
                "title",
                "address",
                "asking_price",
                "total_price",
                "costs",
                "joint_debt",
                "monthly_fee",
                "property_type",
                "ownership",
                "bedrooms",
                "internal_area",
                "usable_area",
                "external_usable_area",
                "floor",
                "build_year",
                "rooms",
                "local_area",
                "area_name",
                "image_0",
                "image_1",
                "image_2",
                "latitude",
                "longitude",
                "scrape_status",
            ]
        )

    def save_finn_codes(self, finn_codes_data: List[Dict[str, Any]]) -> None:
        """Save finn codes to the CSV file."""
        if self.finn_codes_df is None:
            self.initialize()

        # Create a new DataFrame with the new data
        new_df = pd.DataFrame(finn_codes_data)

        # Set default scrape_status to "pending" if not provided
        if "scrape_status" not in new_df.columns:
            new_df["scrape_status"] = "pending"

        # Combine with existing data, using finn_code as the key
        combined_df = pd.concat([self.finn_codes_df, new_df]).drop_duplicates(
            subset=["finn_code"]
        )
        self.finn_codes_df = combined_df

        # Save to CSV
        try:
            self.finn_codes_df.to_csv(self.finn_codes_path, index=False)
            logger.info(
                f"Saved {len(finn_codes_data)} finn codes to {self.finn_codes_path}"
            )
        except Exception as e:
            logger.error(f"Error saving finn codes to {self.finn_codes_path}: {e}")

    def fetch_finn_codes(self, select_all: bool = False) -> List[tuple]:
        """Fetch finn codes from the CSV file."""
        if self.finn_codes_df is None:
            self.initialize()

        try:
            if not select_all:
                # Filter for pending status
                filtered_df = self.finn_codes_df[
                    self.finn_codes_df["scrape_status"] == "pending"
                ]
            else:
                filtered_df = self.finn_codes_df

            # Convert to tuples to match SQLite backend format
            finn_codes = [(row["finn_code"],) for _, row in filtered_df.iterrows()]
            logger.info(f"Fetched {len(finn_codes)} finn codes from CSV")
            return finn_codes
        except Exception as e:
            logger.error(f"Error fetching finn codes from CSV: {e}")
            return []

    def save_property_data(self, property_data: Dict[str, Any]) -> None:
        """Save property data to the CSV file."""
        if self.properties_df is None:
            self.initialize()

        # Create a new DataFrame with the new data
        new_df = pd.DataFrame([property_data])

        # Update existing data or append new data
        finn_code = property_data.get("finn_code")
        if finn_code in self.properties_df["finn_code"].values:
            # Update existing row
            self.properties_df = self.properties_df[
                self.properties_df["finn_code"] != finn_code
            ]

        # Append new data
        self.properties_df = pd.concat([self.properties_df, new_df])

        # Save to CSV
        try:
            self.properties_df.to_csv(self.properties_path, index=False)
            logger.info(
                f"Saved property data for finn code {finn_code} to {self.properties_path}"
            )
        except Exception as e:
            logger.error(f"Error saving property data to {self.properties_path}: {e}")

    def update_finn_code_status(self, finn_code: str, status: str) -> None:
        """Update the scrape status of a finn code in the CSV file."""
        if self.finn_codes_df is None:
            self.initialize()

        try:
            # Find the row with the given finn code
            mask = self.finn_codes_df["finn_code"] == finn_code
            if any(mask):
                # Update the status
                self.finn_codes_df.loc[mask, "scrape_status"] = status
                # Save to CSV
                self.finn_codes_df.to_csv(self.finn_codes_path, index=False)
                logger.info(
                    f"Updated scrape status for finn code {finn_code} to {status}"
                )
            else:
                logger.warning(f"Finn code {finn_code} not found in CSV")
        except Exception as e:
            logger.error(f"Error updating finn code status in CSV: {e}")

    def export_to_csv(self, csv_name: str) -> None:
        """Export properties to a CSV file."""
        if self.properties_df is None:
            self.initialize()

        try:
            self.properties_df.to_csv(csv_name, index=False)
            logger.info(f"Exported properties to {csv_name}")
        except Exception as e:
            logger.error(f"Error exporting properties to {csv_name}: {e}")

    def export_finn_codes_to_csv(self, csv_name: str) -> None:
        """Export finn codes to a CSV file."""
        if self.finn_codes_df is None:
            self.initialize()

        try:
            self.finn_codes_df.to_csv(csv_name, index=False)
            logger.info(f"Exported finn codes to {csv_name}")
        except Exception as e:
            logger.error(f"Error exporting finn codes to {csv_name}: {e}")

    def close(self) -> None:
        """Close the CSV backend (save any pending changes)."""
        if self.finn_codes_df is not None:
            try:
                self.finn_codes_df.to_csv(self.finn_codes_path, index=False)
            except Exception as e:
                logger.error(f"Error saving finn codes on close: {e}")

        if self.properties_df is not None:
            try:
                self.properties_df.to_csv(self.properties_path, index=False)
            except Exception as e:
                logger.error(f"Error saving properties on close: {e}")

        logger.info("CSV backend closed")

    def update_finn_code_status(self, finn_code: str, status: str) -> None:
        """Update the scrape status of a finn code in the CSV file."""
        if self.finn_codes_df is None:
            self.initialize()

        try:
            # Find the row with the given finn code
            mask = self.finn_codes_df["finn_code"] == finn_code
            if any(mask):
                # Update the status
                self.finn_codes_df.loc[mask, "scrape_status"] = status
                # Save to CSV
                self.finn_codes_df.to_csv(self.finn_codes_path, index=False)
                logger.info(
                    f"Updated scrape status for finn code {finn_code} to {status}"
                )
            else:
                logger.warning(f"Finn code {finn_code} not found in CSV")
        except Exception as e:
            logger.error(f"Error updating finn code status in CSV: {e}")

    def update_finn_code_status_fields(self, finn_code: str, **kwargs) -> None:
        """Updates multiple status fields of a finn_code in the CSV."""
        if self.finn_codes_df is None:
            self.initialize()

        if not kwargs:
            return

        try:
            # Find the row with the given finn code
            mask = self.finn_codes_df["finn_code"] == finn_code
            if any(mask):
                # Update each field
                for key, value in kwargs.items():
                    self.finn_codes_df.loc[mask, key] = value

                # Save to CSV
                self.finn_codes_df.to_csv(self.finn_codes_path, index=False)
                logger.info(
                    f"Updated status fields for Finn code {finn_code}: {kwargs}"
                )
            else:
                logger.warning(f"Finn code {finn_code} not found in CSV")
        except Exception as e:
            logger.error(f"Error updating finn code status fields in CSV: {e}")

    def fetch_finn_codes_with_status(self, status: str = None) -> List[tuple]:
        """
        Fetches Finn codes with a specific listing status.

        Args:
            status: Listing status to filter by (e.g., 'active', 'inactive')

        Returns:
            List of (finn_code, last_date_checked) tuples
        """
        if self.finn_codes_df is None:
            self.initialize()

        try:
            if status:
                filtered_df = self.finn_codes_df[
                    self.finn_codes_df["listing_status"] == status
                ]
            else:
                filtered_df = self.finn_codes_df

            # Convert to list of tuples (finn_code, last_date_checked)
            result = [
                (row["finn_code"], row["last_date_checked"])
                for _, row in filtered_df.iterrows()
            ]

            logger.info(
                f"Fetched {len(result)} Finn codes with status {status or 'any'}"
            )
            return result
        except Exception as e:
            logger.error(
                f"Error fetching finn codes with status {status} from CSV: {e}"
            )
            return []
