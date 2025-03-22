#!/usr/bin/env python
"""
Script for updating finn codes status (active/inactive).

This script has two main modes:
1. Update active status: Scrapes finn codes and marks them as active
2. Mark inactive listings: Identifies old listings and marks them as inactive
"""
import sys
import argparse
import logging
import datetime

from scraper import utils
from scraper.storage.factory import create_storage_backend
from scraper.finn_code_scraper import fetch_finn_codes
from scraper.services.finn_code_manager import FinnCodeManager

logger = logging.getLogger(__name__)


def update_active_status(args, config):
    """
    Scrape finn codes and update their active status.
    """
    # Create storage
    storage = create_storage_backend(config)
    storage.initialize()

    try:
        # Fetch finn codes from Finn.no
        logger.info("Fetching finn codes from Finn.no")
        finn_codes = fetch_finn_codes(config)

        if not finn_codes:
            logger.warning("No finn codes found on Finn.no")
            return

        logger.info(f"Found {len(finn_codes)} finn codes on Finn.no")

        # Create finn code manager
        manager = FinnCodeManager(storage)

        # Update active status
        result = manager.update_active_finn_codes(finn_codes)

        logger.info(
            f"Update complete: {result['new']} new codes, {result['updated']} updated codes"
        )

    finally:
        storage.close()


def mark_inactive_listings(args, config):
    """
    Mark old listings as inactive.
    """
    # Create storage
    storage = create_storage_backend(config)
    storage.initialize()

    try:
        # Create finn code manager
        manager = FinnCodeManager(storage)

        # Mark inactive listings
        days = args.days or 1
        inactive_count = manager.mark_inactive_listings(days_threshold=days)

        logger.info(
            f"Marked {inactive_count} listings as inactive (not seen in {days} days)"
        )

    finally:
        storage.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Update finn codes status")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument(
        "--backend",
        choices=["sqlite", "csv", "supabase"],
        help="Storage backend type (overrides config)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Active status command
    active_parser = subparsers.add_parser(
        "active", help="Update active status of finn codes"
    )

    # Inactive status command
    inactive_parser = subparsers.add_parser(
        "inactive", help="Mark old listings as inactive"
    )
    inactive_parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days after which to mark as inactive",
    )

    args = parser.parse_args()

    # Setup
    utils.setup_logging()
    config = utils.load_config()

    if args.backend:
        config["backend"] = args.backend

    # Execute command
    if args.command == "active":
        update_active_status(args, config)
    elif args.command == "inactive":
        mark_inactive_listings(args, config)
    else:
        logger.error("No command specified")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
