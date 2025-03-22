import logging
import datetime
from typing import List, Dict, Any, Optional, Tuple

from scraper.storage.base import StorageBackend
from scraper.property_details_scraper import scrape_property_details
from scraper.services.property_service import PropertyService

logger = logging.getLogger(__name__)


class PropertyManager:
    """
    Service for managing property data based on finn code status.
    """

    def __init__(self, storage: StorageBackend, config: Dict[str, Any]):
        """
        Initialize the property manager.

        Args:
            storage: Storage backend instance
            config: Configuration dictionary
        """
        self.storage = storage
        self.config = config
        self.property_service = PropertyService(config)

    def process_property_for_finn_code(
        self, finn_code: str, is_active: bool = True
    ) -> Tuple[bool, Optional[str]]:
        """
        Process property information for a single finn code.

        Args:
            finn_code: The finn code to process
            is_active: Whether the finn code is currently marked as active

        Returns:
            Tuple of (success, status_message)
        """
        try:
            # Check if property exists
            property_exists = self.check_property_exists(finn_code)

            # If inactive, we can skip processing
            if not is_active and property_exists:
                return True, "Skipped inactive property"

            # Scrape property if it doesn't exist or is active
            if not property_exists or is_active:
                property_data = self.property_service.scrape_property(finn_code)

                if not property_data:
                    logger.warning(
                        f"Failed to scrape property data for finn code: {finn_code}"
                    )
                    self.storage.update_finn_code_status(finn_code, "error")
                    return False, "Failed to scrape property"

                # Add timestamp
                property_data["last_date_checked"] = datetime.datetime.now().isoformat()

                # Check if property is inactive using dummy function for now
                is_inactive = self.check_property_inactive(property_data)

                if is_inactive:
                    # Update finn code status to inactive
                    self.storage.update_finn_code_status_fields(
                        finn_code, scrape_status="success", listing_status="inactive"
                    )
                    property_data["listing_status"] = "inactive"
                    logger.info(f"Property {finn_code} marked as inactive")
                else:
                    # Update finn code status
                    self.storage.update_finn_code_status_fields(
                        finn_code,
                        scrape_status="success",
                        listing_status="active",
                        last_date_checked=datetime.datetime.now().isoformat(),
                    )
                    property_data["listing_status"] = "active"

                # Save property data
                self.storage.save_property_data(property_data)

                return True, "Property processed successfully"

            return True, "No action needed"

        except Exception as e:
            logger.error(f"Error processing property for finn code {finn_code}: {e}")
            self.storage.update_finn_code_status(finn_code, "error")
            return False, f"Error: {str(e)}"

    def check_property_exists(self, finn_code: str) -> bool:
        """
        Check if a property exists in the properties table.

        Args:
            finn_code: Finn code to check

        Returns:
            True if property exists, False otherwise
        """
        # This method will vary based on your storage implementation
        # Implement a method in your storage backends to check for existence
        try:
            if hasattr(self.storage, "property_exists"):
                return self.storage.property_exists(finn_code)

            # Fallback implementation if method doesn't exist
            properties = self.storage.fetch_properties(finn_codes=[finn_code])
            return len(properties) > 0
        except Exception as e:
            logger.error(f"Error checking if property {finn_code} exists: {e}")
            return False

    def check_property_inactive(self, property_data: Dict[str, Any]) -> bool:
        """
        Dummy function to check if a property is inactive.
        In a real implementation, this would analyze the property data to determine if
        it's no longer active (e.g., by looking for 'sold' status indicators).

        Args:
            property_data: Property data dictionary

        Returns:
            True if property is inactive, False otherwise
        """
        # Dummy implementation - look for specific signals in data
        # Look for indicators like "SOLGT" in the title or a sold status field

        # Check title for sold indicator
        title = property_data.get("title", "").lower()
        if "solgt" in title or "sold" in title:
            return True

        # Check status field if it exists
        status = property_data.get("status", "").lower()
        if status == "solgt" or status == "sold":
            return True

        # Could also check price changes, availability, etc.

        return False

    def process_all_properties(
        self, limit: Optional[int] = None, process_inactive: bool = False
    ) -> Dict[str, int]:
        """
        Process all properties based on finn codes.

        Args:
            limit: Maximum number of properties to process
            process_inactive: Whether to process inactive finn codes

        Returns:
            Statistics dictionary with counts of processed properties
        """
        # Get all finn codes
        all_finn_codes = self.storage.fetch_finn_codes(select_all=True)

        if limit:
            all_finn_codes = all_finn_codes[:limit]

        stats = {
            "total": len(all_finn_codes),
            "processed": 0,
            "success": 0,
            "error": 0,
            "skipped": 0,
        }

        for i, code_data in enumerate(all_finn_codes):
            finn_code = code_data[0]

            # Check if this finn code should be processed
            process = True
            is_active = True

            # If we have listing_status information (depends on the storage implementation)
            if len(code_data) > 1:
                # Assume index 2 might contain listing_status if available
                status_idx = 2 if len(code_data) > 2 else 1
                status = (
                    code_data[status_idx] if len(code_data) > status_idx else "active"
                )
                is_active = status == "active"

                if not is_active and not process_inactive:
                    stats["skipped"] += 1
                    logger.info(f"Skipping inactive finn code: {finn_code}")
                    continue

            logger.info(
                f"Processing property {i + 1}/{len(all_finn_codes)}: {finn_code}"
            )

            success, message = self.process_property_for_finn_code(finn_code, is_active)
            stats["processed"] += 1

            if success:
                stats["success"] += 1
            else:
                stats["error"] += 1
                logger.warning(f"Failed to process property {finn_code}: {message}")

        return stats
