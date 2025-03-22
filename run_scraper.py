#!/usr/bin/env python
"""
Main entry point for the Finn.no scraper.
"""
import sys
import argparse
import logging

from scraper.app import FinnScraperApp
from scraper.logger import setup_logging, get_logger
from scraper.errors import ScraperError
from scraper.storage import create_storage_backend

logger = get_logger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Finn.no Property Scraper")

    # Global options
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument(
        "--backend",
        choices=["sqlite", "csv", "supabase"],
        help="Storage backend type (overrides config)",
    )
    parser.add_argument(
        "--export-finn-codes",
        metavar="PATH",
        help="Export finn codes to CSV file at this path",
    )
    parser.add_argument(
        "--export-properties",
        metavar="PATH",
        help="Export properties to CSV file at this path",
    )

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Finn codes command
    finn_codes_parser = subparsers.add_parser("finn-codes", help="Scrape finn codes")
    finn_codes_parser.add_argument(
        "--drop-table",
        action="store_true",
        help="Drop existing finn codes table before scraping",
    )

    # Properties command
    properties_parser = subparsers.add_parser(
        "properties", help="Scrape property details"
    )
    properties_parser.add_argument(
        "--drop-table",
        action="store_true",
        help="Drop existing properties table before scraping",
    )
    properties_parser.add_argument(
        "--limit", type=int, help="Limit number of properties to scrape"
    )
    properties_parser.add_argument(
        "--all",
        action="store_true",
        help="Scrape all finn codes, not just pending ones",
    )

    # Full scrape command
    full_parser = subparsers.add_parser("full", help="Run full scraping process")
    full_parser.add_argument(
        "--drop-tables",
        action="store_true",
        help="Drop existing tables before scraping",
    )
    full_parser.add_argument(
        "--limit", type=int, help="Limit number of properties to scrape"
    )

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_args()
    setup_logging()

    if not args.command:
        logger.error("No command specified")
        sys.exit(1)

    try:
        # Create the application
        app = FinnScraperApp(args.config)

        # Override backend if specified
        if args.backend:
            app.config["backend"] = args.backend
            logger.info(f"Using backend: {args.backend}")

            # Re-create storage with new backend
            app.storage = create_storage_backend(app.config)

        # Initialize the app
        app.initialize()

        # Execute the command
        if args.command == "finn-codes":
            app.scrape_finn_codes(drop_table=args.drop_table)

        elif args.command == "properties":
            app.scrape_properties(select_all=args.all, limit=args.limit)

        elif args.command == "full":
            # First scrape finn codes
            if args.drop_tables:
                if hasattr(app.storage, "drop_finn_codes_table"):
                    app.storage.drop_finn_codes_table()
                if hasattr(app.storage, "drop_properties_table"):
                    app.storage.drop_properties_table()
                logger.info("Dropped existing tables")

            app.scrape_finn_codes()
            app.scrape_properties(limit=args.limit)

        # Export data if requested
        if args.export_finn_codes:
            app.storage.export_finn_codes_to_csv(args.export_finn_codes)
            logger.info(f"Exported finn codes to {args.export_finn_codes}")

        if args.export_properties:
            app.storage.export_to_csv(args.export_properties)
            logger.info(f"Exported properties to {args.export_properties}")

        # Close the app
        app.close()

        logger.info("Scraper completed successfully")

    except ScraperError as e:
        logger.error(f"Scraper error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
