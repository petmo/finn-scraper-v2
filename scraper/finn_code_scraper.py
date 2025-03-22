import requests
from bs4 import BeautifulSoup
import time
import random
import logging

logger = logging.getLogger(__name__)


def fetch_finn_codes(config):
    """
    Fetches Finn codes from the base URL specified in the config, iterating through pages.

    Args:
        config: The configuration dictionary.

    Returns:
        A set of unique Finn codes found.
    """
    finn_codes = set()
    base_url = config.get("base_url")
    max_page = config.get("max_page", 50)  # Default to 50 if not specified
    finn_code_selector = config.get(
        "finn_code_selector", 'a[href*="finnkode="]'
    )  # Default selector

    if not base_url:
        logger.error("No base URL specified in the config for fetching Finn codes.")
        return finn_codes

    scrape_delay_min = config.get("scrape_delay_min", 1)
    scrape_delay_max = config.get("scrape_delay_max", 3)

    logger.info(f"Fetching Finn codes from base URL: {base_url}, up to page {max_page}")

    for page_number in range(1, max_page + 1):
        url = (
            f"{base_url}&page={page_number}"
            if "page=" not in base_url
            else base_url.replace("page=", f"page={page_number}")
        )
        logger.info(f"Fetching page: {url}")
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            # Find all elements matching the Finn code selector
            link_elements = soup.select(finn_code_selector)

            found_on_page = False
            for link in link_elements:
                href = link.get("href")
                if href and "finnkode=" in href:
                    finn_code = href.split("finnkode=")[1].split("&")[0]
                    finn_codes.add(finn_code)
                    found_on_page = True

            if not found_on_page and page_number == 1:
                logger.warning(
                    f"No elements found with selector '{finn_code_selector}' on the first page of {base_url}. Check your config."
                )
                break  # Stop if no links are found on the first page

            if not found_on_page and page_number > 1:
                logger.info(
                    f"No more Finn codes found on page {page_number} of {base_url}."
                )
                break  # Stop if no Finn codes found on the current page

            time.sleep(random.uniform(scrape_delay_min, scrape_delay_max))

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Finn codes from {url}: {e}")
            break  # Stop fetching on error for the current base URL

    logger.info(f"Found a total of {len(finn_codes)} unique Finn codes.")
    return finn_codes
