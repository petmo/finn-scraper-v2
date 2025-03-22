import requests
from bs4 import BeautifulSoup
import time
import random
import logging

from scraper.parsers.property_parser import PropertyParser
from scraper.geocoding import geocode_address

logger = logging.getLogger(__name__)


def scrape_property_details(finn_code, config):
    """Scrapes property details for a given Finn code."""
    property_data = {
        "finn_code": finn_code,
        "scrape_status": "pending",
    }  # Initialize scrape_status
    ad_url = config["ad_url"].format(finn_code)

    logger.info(f"Scraping property details from: {ad_url}")
    scrape_delay_min = config["scrape_delay_min"]
    scrape_delay_max = config["scrape_delay_max"]

    try:
        response = requests.get(ad_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        # print(response.content) # Print HTML for inspection - keep for debugging

        parser = PropertyParser()
        property_data = parser.parse(soup)

        time.sleep(random.uniform(scrape_delay_min, scrape_delay_max))
        property_data["scrape_status"] = (
            "success"  # Update status to success if scraping completes
        )

    except requests.exceptions.RequestException as e:
        logger.error(f"Error scraping Finn code {finn_code}: {e}")
        property_data["scrape_status"] = "failed"  # Update status to failed on error
        return None  # Indicate scraping failed for this finn_code

    return property_data
