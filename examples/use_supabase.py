#!/usr/bin/env python
"""
Example script showing how to use the Supabase backend.
"""

import sys
import os
import logging

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scraper import utils
from scraper.storage.supabase_backend import SupabaseBackend

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_supabase():
    """
    Example function to set up and test the Supabase connection.
    """
    # Load configuration
    config = utils.load_config()

    # Get Supabase configuration
    supabase_config = config.get("supabase", {})
    url = supabase_config.get("url")
    key = supabase_config.get("key")

    if (
        not url
        or url == "YOUR_SUPABASE_URL"
        or not key
        or key == "YOUR_SUPABASE_API_KEY"
    ):
        logger.error("Supabase URL and key not configured in config.yaml")
        logger.info(
            "Please update config/config.yaml with your Supabase URL and API key"
        )
        sys.exit(1)

    # Create and initialize the backend
    backend = SupabaseBackend(url, key)

    try:
        logger.info("Initializing Supabase connection...")
        backend.initialize()
        logger.info("Successfully connected to Supabase!")

        # Test fetching finn codes
        logger.info("Testing finn code fetch...")
        finn_codes = backend.fetch_finn_codes()
        logger.info(f"Successfully fetched {len(finn_codes)} finn codes")

        # Close the connection
        backend.close()
        logger.info("Supabase connection closed")

    except Exception as e:
        logger.error(f"Error connecting to Supabase: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    setup_supabase()
