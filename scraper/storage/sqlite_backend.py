import sqlite3
import csv
import logging
from typing import List, Dict, Any, Optional, Union

from scraper.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class SQLiteBackend(StorageBackend):
    """
    SQLite storage backend implementation.
    """

    def __init__(self, db_name: str):
        """Initialize the SQLite backend with a database name."""
        self.db_name = db_name
        self.conn = None

    def initialize(self) -> None:
        """Initialize the SQLite database connection and create tables."""
        self.conn = self._create_connection()
        if self.conn:
            self._create_table_finn_codes()
            self._create_table_properties()
            logger.info(f"SQLite backend initialized with database: {self.db_name}")
        else:
            logger.error(
                f"Failed to initialize SQLite backend with database: {self.db_name}"
            )

    def _create_connection(self):
        """Creates a database connection to a SQLite database."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            logger.info(f"Connected to database: {self.db_name}")
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
        return conn

    def _create_table_finn_codes(self):
        """Creates the finn_codes table in the database if it doesn't exist."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS finn_codes (
                    finn_code TEXT PRIMARY KEY,
                    fetched_at TEXT,
                    scrape_status TEXT DEFAULT 'pending'
                )
            """
            )
            self.conn.commit()
            logger.info("Table 'finn_codes' created or already exists.")
        except sqlite3.Error as e:
            logger.error(f"Database error creating finn_codes table: {e}")

    def _create_table_properties(self):
        """Creates the properties table in the database if it doesn't exist."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS properties (
                    finn_code TEXT PRIMARY KEY,
                    title TEXT,
                    address TEXT,
                    asking_price TEXT,
                    total_price TEXT,
                    costs TEXT,
                    joint_debt TEXT,
                    monthly_fee TEXT,
                    property_type TEXT,
                    ownership TEXT,
                    bedrooms TEXT,
                    internal_area TEXT,
                    usable_area TEXT,
                    external_usable_area TEXT,
                    floor TEXT,
                    build_year TEXT,
                    rooms TEXT,
                    local_area TEXT,
                    area_name TEXT,
                    image_0 TEXT,
                    image_1 TEXT,
                    image_2 TEXT,
                    latitude TEXT,
                    longitude TEXT,
                    scrape_status TEXT DEFAULT 'pending'
                )
            """
            )
            self.conn.commit()
            logger.info("Table 'properties' created or already exists.")
        except sqlite3.Error as e:
            logger.error(f"Database error creating properties table: {e}")

    def drop_finn_codes_table(self):
        """Drops the finn_codes table if it exists."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS finn_codes")
            self.conn.commit()
            logger.info("Existing table 'finn_codes' dropped.")
        except sqlite3.Error as e:
            logger.error(f"Database error dropping finn_codes table: {e}")

    def drop_properties_table(self):
        """Drops the properties table if it exists."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS properties")
            self.conn.commit()
            logger.info("Existing table 'properties' dropped.")
        except sqlite3.Error as e:
            logger.error(f"Database error dropping properties table: {e}")

    def save_finn_codes(self, finn_codes_data: List[Dict[str, Any]]) -> None:
        """Saves fetched finn codes to the database."""
        sql = """
            INSERT OR IGNORE INTO finn_codes (finn_code, fetched_at, scrape_status) 
            VALUES (?, ?, ?)
        """
        try:
            cursor = self.conn.cursor()
            cursor.executemany(
                sql,
                [
                    (
                        item["finn_code"],
                        item["fetched_at"],
                        item.get("scrape_status", "pending"),
                    )
                    for item in finn_codes_data
                ],
            )
            self.conn.commit()
            logger.info(f"Saved {len(finn_codes_data)} Finn codes to database.")
        except sqlite3.Error as e:
            logger.error(f"Database error saving finn codes: {e}")

    def fetch_finn_codes(self, select_all: bool = False) -> List[tuple]:
        """Fetches Finn codes from the database that have not been scraped yet."""
        if not select_all:
            query = "SELECT finn_code FROM finn_codes WHERE scrape_status = 'pending'"
        else:
            query = "SELECT finn_code FROM finn_codes"

        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            finn_codes = cursor.fetchall()
            logger.info(
                f"Fetched {len(finn_codes)} Finn codes from database for scraping."
            )
            return finn_codes
        except sqlite3.Error as e:
            logger.error(f"Database error fetching finn codes: {e}")
            return []

    def save_property_data(self, property_data: Dict[str, Any]) -> None:
        """Saves property data to the database."""
        columns = [
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

        placeholders = ", ".join(["?"] * len(columns))
        columns_str = ", ".join(columns)

        sql = f"""
            INSERT OR REPLACE INTO properties ({columns_str})
            VALUES ({placeholders})
        """

        try:
            cursor = self.conn.cursor()
            values = [property_data.get(col) for col in columns]
            cursor.execute(sql, values)
            self.conn.commit()
            logger.info(
                f"Saved property data for Finn code: {property_data.get('finn_code')}"
            )
        except sqlite3.Error as e:
            logger.error(f"Database error saving property data: {e}")

    def update_finn_code_status(self, finn_code: str, status: str) -> None:
        """Updates the scrape status of a finn_code in the finn_codes table."""
        sql = """
            UPDATE finn_codes SET scrape_status = ? WHERE finn_code = ?
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, (status, finn_code))
            self.conn.commit()
            logger.info(f"Updated scrape status for Finn code {finn_code} to: {status}")
        except sqlite3.Error as e:
            logger.error(
                f"Database error updating scrape status for Finn code {finn_code}: {e}"
            )

    def export_to_csv(self, csv_name: str) -> None:
        """Exports data from the properties table to a CSV file."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM properties")
            rows = cursor.fetchall()
            if rows:
                with open(csv_name, "w", newline="", encoding="utf-8") as csvfile:
                    csv_writer = csv.writer(csvfile)
                    csv_writer.writerow(
                        [description[0] for description in cursor.description]
                    )
                    csv_writer.writerows(rows)
                logger.info(f"Property data exported to CSV file: {csv_name}")
            else:
                logger.info("No property data to export from the database.")
        except sqlite3.Error as e:
            logger.error(f"Database error exporting property data to CSV: {e}")

    def export_finn_codes_to_csv(self, csv_name: str) -> None:
        """Exports data from the finn_codes table to a CSV file."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM finn_codes")
            rows = cursor.fetchall()
            if rows:
                with open(csv_name, "w", newline="", encoding="utf-8") as csvfile:
                    csv_writer = csv.writer(csvfile)
                    csv_writer.writerow(
                        [description[0] for description in cursor.description]
                    )
                    csv_writer.writerows(rows)
                logger.info(f"Finn codes exported to CSV file: {csv_name}")
            else:
                logger.info("No finn codes to export from the database.")
        except sqlite3.Error as e:
            logger.error(f"Database error exporting finn codes to CSV: {e}")

    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")

    def update_finn_code_status(self, finn_code: str, status: str) -> None:
        """Updates the scrape status of a finn_code in the finn_codes table."""
        sql = """
            UPDATE finn_codes SET scrape_status = ? WHERE finn_code = ?
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, (status, finn_code))
            self.conn.commit()
            logger.info(f"Updated scrape status for Finn code {finn_code} to: {status}")
        except sqlite3.Error as e:
            logger.error(
                f"Database error updating scrape status for Finn code {finn_code}: {e}"
            )

    def update_finn_code_status_fields(self, finn_code: str, **kwargs) -> None:
        """Updates multiple status fields of a finn_code in the finn_codes table."""
        if not kwargs:
            return

        # Build the SET clause from kwargs
        set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(finn_code)

        sql = f"""
            UPDATE finn_codes SET {set_clause} WHERE finn_code = ?
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, values)
            self.conn.commit()
            logger.info(f"Updated status fields for Finn code {finn_code}: {kwargs}")
        except sqlite3.Error as e:
            logger.error(
                f"Database error updating status fields for Finn code {finn_code}: {e}"
            )

    def fetch_finn_codes_with_status(self, status: str = None) -> List[tuple]:
        """
        Fetches Finn codes with a specific listing status.

        Args:
            status: Listing status to filter by (e.g., 'active', 'inactive')

        Returns:
            List of (finn_code, last_date_checked) tuples
        """
        if status:
            query = "SELECT finn_code, last_date_checked FROM finn_codes WHERE listing_status = ?"
            params = (status,)
        else:
            query = "SELECT finn_code, last_date_checked FROM finn_codes"
            params = ()

        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            finn_codes = cursor.fetchall()
            logger.info(
                f"Fetched {len(finn_codes)} Finn codes with status {status or 'any'}"
            )
            return finn_codes
        except sqlite3.Error as e:
            logger.error(
                f"Database error fetching finn codes with status {status}: {e}"
            )
            return []

    def property_exists(self, finn_code: str) -> bool:
        """Check if a property exists in the properties table."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM properties WHERE finn_code = ?", (finn_code,))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Database error checking property existence: {e}")
            return False

    def fetch_properties(
        self, finn_codes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch properties from the database, optionally filtered by finn codes."""
        try:
            cursor = self.conn.cursor()

            if finn_codes:
                # Create placeholders for IN clause
                placeholders = ", ".join(["?"] * len(finn_codes))
                query = f"SELECT * FROM properties WHERE finn_code IN ({placeholders})"
                cursor.execute(query, finn_codes)
            else:
                cursor.execute("SELECT * FROM properties")

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Convert rows to dictionaries
            properties = []
            for row in cursor.fetchall():
                property_dict = dict(zip(columns, row))
                properties.append(property_dict)

            logger.info(f"Fetched {len(properties)} properties from database")
            return properties
        except sqlite3.Error as e:
            logger.error(f"Database error fetching properties: {e}")
            return []
