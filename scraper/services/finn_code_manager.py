import logging
import datetime
from typing import List, Dict, Any, Set
from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from scraper.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class FinnCodeManager:
    """
    Service for managing finn code lifecycle (active/inactive status).
    """

    def __init__(self, storage: StorageBackend):
        """
        Initialize the finn code manager.

        Args:
            storage: Storage backend instance
        """
        self.storage = storage

    def update_active_finn_codes(self, found_finn_codes: Set[str]) -> Dict[str, int]:
        """
        Update the active status of finn codes based on currently found codes.

        Args:
            found_finn_codes: Set of finn codes found in the current scrape

        Returns:
            Dictionary with counts of new, updated, and unchanged finn codes
        """
        today = datetime.datetime.now().isoformat()
        new_count = 0
        updated_count = 0

        # Fetch all existing finn codes
        existing_codes = self.storage.fetch_finn_codes(select_all=True)
        existing_finn_codes = {code[0] for code in existing_codes}

        # Process found codes
        finn_codes_data = []

        for finn_code in found_finn_codes:
            if finn_code in existing_finn_codes:
                # Update existing code to active
                self.storage.update_finn_code_status_fields(
                    finn_code, listing_status="active", last_date_checked=today
                )
                updated_count += 1
            else:
                # Create new code
                finn_codes_data.append(
                    {
                        "finn_code": finn_code,
                        "fetched_at": today,
                        "scrape_status": "pending",
                        "listing_status": "active",
                        "last_date_checked": today,
                    }
                )
                new_count += 1

        # Save new codes
        if finn_codes_data:
            self.storage.save_finn_codes(finn_codes_data)

        return {
            "new": new_count,
            "updated": updated_count,
            "total_found": len(found_finn_codes),
        }

    def mark_inactive_listings(self, days_threshold: int = 1) -> int:
        """
        Mark listings as inactive if they haven't been seen for a certain number of days.

        Args:
            days_threshold: Number of days after which to mark as inactive

        Returns:
            Number of listings marked as inactive
        """
        # Get the cutoff date
        cutoff_date = (
            datetime.datetime.now() - datetime.timedelta(days=days_threshold)
        ).isoformat()

        # Fetch all active finn codes
        all_codes = self.storage.fetch_finn_codes_with_status(status="active")
        inactive_count = 0

        for code_data in all_codes:
            finn_code = code_data[0]
            last_checked = code_data[1]  # Assuming this is index of last_date_checked

            if last_checked and last_checked < cutoff_date:
                # Mark as inactive
                self.storage.update_finn_code_status_fields(
                    finn_code, listing_status="inactive"
                )
                inactive_count += 1
                logger.info(
                    f"Marked {finn_code} as inactive (last checked: {last_checked})"
                )

        return inactive_count
