#!/usr/bin/env python
"""
Script for processing properties based on finn code status.
"""
import sys
import argparse
import logging
import datetime

from dotenv import load_dotenv

from scraper import utils
from scraper.storage.factory import create_storage_backend
from scraper.services.property_manager import PropertyManager

logger = logging.getLogger(__name__)
load_dotenv(".env")


def process_properties(args, config):
    """
    Process property data based on finn codes.
    """
    # Create storage
    storage = create_storage_backend(config)
    storage.initialize()

    try:
        # Create property manager
        manager = PropertyManager(storage, config)

        # Process properties
        limit = args.limit if hasattr(args, "limit") else None
        process_inactive = args.inactive if hasattr(args, "inactive") else False

        logger.info(
            f"Starting property processing (limit: {limit}, process_inactive: {process_inactive})"
        )

        stats = manager.process_all_properties(
            limit=limit, process_inactive=process_inactive
        )

        logger.info(f"Property processing complete:")
        logger.info(f"  Total: {stats['total']}")
        logger.info(f"  Processed: {stats['processed']}")
        logger.info(f"  Success: {stats['success']}")
        logger.info(f"  Error: {stats['error']}")
        logger.info(f"  Skipped: {stats['skipped']}")

        # Print more readable stats to the console
        print("\n===== Property Processing Results =====")
        print(f"Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total Finn Codes: {stats['total']}")
        print(f"Processed: {stats['processed']}")
        print(f"Successfully Processed: {stats['success']}")
        print(f"Errors: {stats['error']}")
        print(f"Skipped (inactive): {stats['skipped']}")
        print("=====================================\n")

    finally:
        storage.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Process properties based on finn codes"
    )
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument(
        "--backend",
        choices=["sqlite", "csv", "supabase"],
        help="Storage backend type (overrides config)",
    )
    parser.add_argument(
        "--limit", type=int, help="Limit number of properties to process"
    )
    parser.add_argument(
        "--inactive", action="store_true", help="Process inactive finn codes as well"
    )

    args = parser.parse_args()

    # Setup
    utils.setup_logging()
    config = utils.load_config()

    if args.backend:
        config["backend"] = args.backend

    # Process properties
    process_properties(args, config)


if __name__ == "__main__":
    main()
