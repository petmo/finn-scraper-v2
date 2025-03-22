#!/usr/bin/env python
"""
Script for generating a report on finn code status.
"""
import sys
import argparse
import logging
from tabulate import tabulate
import pandas as pd
import datetime

from scraper import utils
from scraper.storage.factory import create_storage_backend

logger = logging.getLogger(__name__)


def generate_status_report(config):
    """
    Generate a status report of finn codes.
    """
    # Create storage
    storage = create_storage_backend(config)
    storage.initialize()

    try:
        # Get data for report
        active_codes = storage.fetch_finn_codes_with_status("active")
        inactive_codes = storage.fetch_finn_codes_with_status("inactive")

        # Fetch all finn codes to get scrape status
        all_codes = storage.fetch_finn_codes(select_all=True)

        scrape_status = {}
        # More robust tuple unpacking
        for code_tuple in all_codes:
            finn_code = code_tuple[0]
            status = code_tuple[2] if len(code_tuple) > 2 else "unknown"
            if status not in scrape_status:
                scrape_status[status] = 0
            scrape_status[status] += 1

        # Print report
        print("\n===== FINN CODE STATUS REPORT =====")
        print(f"Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Backend: {config['backend']}")
        print("\nListing Status:")
        print(f"  Active listings: {len(active_codes)}")
        print(f"  Inactive listings: {len(inactive_codes)}")
        print(f"  Total: {len(active_codes) + len(inactive_codes)}")

        print("\nScrape Status:")
        for status, count in scrape_status.items():
            print(f"  {status}: {count}")

        # Last updated
        if active_codes:
            # Convert to DataFrame for easier analysis
            active_df = pd.DataFrame(
                active_codes, columns=["finn_code", "last_date_checked"]
            )

            # Convert to datetime
            active_df["last_date_checked"] = pd.to_datetime(
                active_df["last_date_checked"]
            )

            # Get most recent update
            most_recent = active_df["last_date_checked"].max()

            print("\nLast Update:")
            print(f"  Most recent check: {most_recent}")

            # Check for old listings
            one_day_ago = datetime.datetime.now() - datetime.timedelta(days=1)
            old_listings = active_df[active_df["last_date_checked"] < one_day_ago]

            if not old_listings.empty:
                print(
                    f"\nWARNING: {len(old_listings)} active listings have not been checked in over 24 hours"
                )

        print("\n==================================")

    finally:
        storage.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Generate a report on finn code status"
    )
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument(
        "--backend",
        choices=["sqlite", "csv", "supabase"],
        help="Storage backend type (overrides config)",
    )

    args = parser.parse_args()

    # Setup
    utils.setup_logging()
    config = utils.load_config()

    if args.backend:
        config["backend"] = args.backend

    # Generate report
    generate_status_report(config)


if __name__ == "__main__":
    main()
