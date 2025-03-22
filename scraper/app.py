"""
Main application class for the Finn.no scraper.
"""

import time
import logging
from typing import Optional, Dict, Any, List

from scraper.config_manager import ConfigManager
from scraper.logger import setup_logging, get_logger
from scraper.services.finn_code_manager import FinnCodeManager
from scraper.storage.factory import create_storage_backend
from scraper.services.finn_code_service import FinnCodeService
from scraper.services.property_service import PropertyService
from scraper.errors import handle_exceptions, ScraperError

logger = get_logger(__name__)


class FinnScraperApp:
    """
    Main application class for the Finn.no scraper.

    This class coordinates the different components of the scraper
    and provides a unified interface for running the scraping process.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the scraper application.

        Args:
            config_path: Path to the configuration file
        """
        # Set up logging
        setup_logging()

        # Load configuration
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.load_config()

        # Create storage backend
        self.storage = create_storage_backend(self.config)

        # Create services
        self.finn_code_service = FinnCodeService(self.config)
        self.property_service = PropertyService(self.config)
        self.finn_code_manager = FinnCodeManager(self.storage)

        logger.info("Finn Scraper application initialized")

    def initialize(self) -> None:
        """Initialize the application components."""
        try:
            self.storage.initialize()
            logger.info("Storage backend initialized")
        except Exception as e:
            logger.error(f"Failed to initialize storage: {e}")
            raise ScraperError(f"Storage initialization failed: {e}")

    @handle_exceptions(reraise=True, error_type=ScraperError)
    def scrape_finn_codes(self, drop_table: bool = False) -> List[Dict[str, Any]]:
        """
        Scrape finn codes from Finn.no.

        Args:
            drop_table: Whether to drop existing finn codes table

        Returns:
            List of scraped finn codes
        """
        logger.info("Starting finn code scraping")

        # Drop table if requested
        if drop_table and hasattr(self.storage, "drop_finn_codes_table"):
            self.storage.drop_finn_codes_table()
            logger.info("Dropped finn codes table")

        # Fetch finn codes
        start_time = time.time()
        finn_codes_data = self.finn_code_service.fetch_finn_codes()

        if not finn_codes_data:
            logger.warning("No finn codes found")
            return []

        # Save finn codes
        self.storage.save_finn_codes(finn_codes_data)

        duration = time.time() - start_time
        logger.info(
            f"Finn code scraping completed in {duration:.2f} seconds. Found {len(finn_codes_data)} codes."
        )

        return finn_codes_data

    @handle_exceptions(reraise=True, error_type=ScraperError)
    def scrape_properties(
        self, select_all: bool = False, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape property details for finn codes.

        Args:
            select_all: Whether to scrape all finn codes or only pending ones
            limit: Maximum number of properties to scrape

        Returns:
            List of scraped property data
        """
        logger.info("Starting property scraping")

        # Fetch finn codes to scrape
        finn_codes_to_scrape = self.storage.fetch_finn_codes(select_all=select_all)

        if not finn_codes_to_scrape:
            logger.warning("No finn codes found to scrape")
            return []

        # Limit number of scrapes if specified
        if limit and limit > 0:
            finn_codes_to_scrape = finn_codes_to_scrape[:limit]

        logger.info(f"Scraping {len(finn_codes_to_scrape)} properties")

        # Scrape properties
        properties_data = []
        start_time = time.time()

        for i, finn_code_data in enumerate(finn_codes_to_scrape):
            finn_code = finn_code_data[0]  # Finn code is the first element

            logger.info(
                f"Scraping property {i + 1}/{len(finn_codes_to_scrape)}: {finn_code}"
            )

            try:
                # Scrape property details
                property_data = self.property_service.scrape_property(finn_code)

                if property_data:
                    # Save property data
                    self.storage.save_property_data(property_data)
                    self.storage.update_finn_code_status(finn_code, "success")
                    properties_data.append(property_data)
                else:
                    logger.warning(
                        f"Failed to scrape property for finn code: {finn_code}"
                    )
                    self.storage.update_finn_code_status(finn_code, "failed")

            except Exception as e:
                logger.error(f"Error scraping property for finn code {finn_code}: {e}")
                self.storage.update_finn_code_status(finn_code, "error")

        duration = time.time() - start_time
        logger.info(
            f"Property scraping completed in {duration:.2f} seconds. "
            f"Scraped {len(properties_data)}/{len(finn_codes_to_scrape)} properties."
        )

        return properties_data

    def export_to_csv(
        self,
        finn_codes_path: Optional[str] = None,
        properties_path: Optional[str] = None,
    ) -> None:
        """
        Export data to CSV files.

        Args:
            finn_codes_path: Path for finn codes CSV export
            properties_path: Path for properties CSV export
        """
        if finn_codes_path:
            self.storage.export_finn_codes_to_csv(finn_codes_path)
            logger.info(f"Exported finn codes to {finn_codes_path}")

        if properties_path:
            self.storage.export_to_csv(properties_path)
            logger.info(f"Exported properties to {properties_path}")

    def close(self) -> None:
        """Close the application and release resources."""
        try:
            if self.storage:
                self.storage.close()
                logger.info("Storage connection closed")
        except Exception as e:
            logger.error(f"Error closing storage: {e}")

    def __enter__(self):
        """Context manager enter method."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit method."""
        self.close()
