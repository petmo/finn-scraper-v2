#!/usr/bin/env python
"""
Tool for migrating data between different storage backends.
"""

import argparse
import logging
import sys

from scraper import utils
from scraper.storage.factory import create_storage_backend

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def migrate_finn_codes(source, target):
    """Migrate finn codes from source to target backend."""
    logger.info("Migrating finn codes...")

    # Fetch all finn codes from source
    finn_codes_data = []
    finn_codes = source.fetch_finn_codes(select_all=True)

    if not finn_codes:
        logger.warning("No finn codes found in source backend")
        return

    logger.info(f"Found {len(finn_codes)} finn codes in source backend")

    # Query full data for each finn code
    for finn_code_tuple in finn_codes:
        finn_code = finn_code_tuple[0]

        # We need to query the full data for each finn code
        # This is a simplified approach - in a real implementation you would
        # want to query all fields from the source backend
        try:
            # For SQLite backend
            if hasattr(source, "conn"):
                cursor = source.conn.cursor()
                cursor.execute(
                    "SELECT * FROM finn_codes WHERE finn_code = ?", (finn_code,)
                )
                row = cursor.fetchone()
                if row:
                    # Convert row to dict using column names
                    columns = [desc[0] for desc in cursor.description]
                    finn_code_data = dict(zip(columns, row))
                    finn_codes_data.append(finn_code_data)
            # For CSV backend
            elif hasattr(source, "finn_codes_df"):
                row = source.finn_codes_df[
                    source.finn_codes_df["finn_code"] == finn_code
                ]
                if not row.empty:
                    finn_code_data = row.iloc[0].to_dict()
                    finn_codes_data.append(finn_code_data)
            # For other backends, just create a minimal entry
            else:
                finn_codes_data.append(
                    {"finn_code": finn_code, "scrape_status": "pending"}
                )
        except Exception as e:
            logger.error(f"Error querying data for finn code {finn_code}: {e}")

    # Save finn codes to target
    if finn_codes_data:
        target.save_finn_codes(finn_codes_data)
        logger.info(f"Migrated {len(finn_codes_data)} finn codes to target backend")
    else:
        logger.warning("No finn codes data retrieved for migration")


def migrate_properties(source, target):
    """Migrate properties from source to target backend."""
    logger.info("Migrating properties...")

    # For this simplified version, we'll export to CSV and then import
    # This works for all backend types
    import tempfile
    import os

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
        temp_path = temp_file.name

    try:
        # Export properties from source to temp CSV
        source.export_to_csv(temp_path)

        # Import from CSV to pandas
        import pandas as pd

        try:
            properties_df = pd.read_csv(temp_path)
            if properties_df.empty:
                logger.warning("No properties found in source backend")
                return

            logger.info(f"Found {len(properties_df)} properties in source backend")

            # Convert DataFrame to list of dicts and save to target
            properties_data = properties_df.to_dict("records")
            for property_data in properties_data:
                target.save_property_data(property_data)

            logger.info(f"Migrated {len(properties_data)} properties to target backend")
        except Exception as e:
            logger.error(f"Error reading properties CSV: {e}")
    finally:
        # Clean up temp file
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def main(args):
    config = utils.load_config()

    # Override source and target backends from command line
    config_source = config.copy()
    config_source["backend"] = args.source

    config_target = config.copy()
    config_target["backend"] = args.target

    logger.info(f"Migrating data from {args.source} to {args.target}")

    # Create source and target backends
    source = create_storage_backend(config_source)
    target = create_storage_backend(config_target)

    try:
        # Initialize backends
        source.initialize()
        target.initialize()

        # Migrate finn codes
        if args.finn_codes:
            migrate_finn_codes(source, target)

        # Migrate properties
        if args.properties:
            migrate_properties(source, target)

        logger.info("Migration completed successfully")

    except Exception as e:
        logger.error(f"Error during migration: {e}", exc_info=True)
        sys.exit(1)

    finally:
        # Close backends
        source.close()
        target.close()
        logger.info("Closed backend connections")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate data between storage backends"
    )
    parser.add_argument(
        "source", choices=["sqlite", "csv", "supabase"], help="Source backend type"
    )
    parser.add_argument(
        "target", choices=["sqlite", "csv", "supabase"], help="Target backend type"
    )
    parser.add_argument(
        "--finn-codes", action="store_true", help="Migrate finn codes data"
    )
    parser.add_argument(
        "--properties", action="store_true", help="Migrate properties data"
    )

    args = parser.parse_args()

    # Ensure at least one data type is selected
    if not (args.finn_codes or args.properties):
        parser.error(
            "At least one data type must be selected (--finn-codes or --properties)"
        )

    main(args)
