import requests
from bs4 import BeautifulSoup
import time
import random
import logging

from scraper.property_parser import PropertyParser


from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import logging


logger = logging.getLogger(__name__)

def geocode_address(address, retries=3, delay=2):
    """
    Geocodes an address using Nominatim service via geopy to get latitude and longitude.

    Args:
        address: The address string to geocode.
        retries: Number of retries for geocoding.
        delay: Delay in seconds between retries.

    Returns:
        A dictionary containing 'latitude' and 'longitude' if geocoding is successful,
        otherwise None.
    """
    geolocator = Nominatim(user_agent="finn_property_scraper") # Set a user agent

    for attempt in range(retries):
        try:
            location = geolocator.geocode(address, timeout=10) # Geocode with timeout
            if location:
                return {'latitude': location.latitude, 'longitude': location.longitude}
            else:
                logger.warning(f"Geocoding failed for address: '{address}'. No location found by Nominatim.")
                return None # Geocoding failed, no location found

        except GeocoderTimedOut:
            if attempt < retries - 1:
                logger.warning(f"Geocoding timed out for address: '{address}'. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Geocoding timed out for address: '{address}' after {retries} retries.")
                return None # Geocoding timed out after retries

        except GeocoderServiceError as e:
            if attempt < retries - 1 and e.status_code == 429: # Rate limit error (429)
                retry_delay = delay * (attempt + 1) # Exponential backoff
                logger.warning(f"Geocoding service error (likely rate limited) for address: '{address}'. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Geocoding service error for address: '{address}'. Error: {e}")
                return None # Geocoding service error

        except Exception as e:
            logger.error(f"An unexpected error occurred during geocoding for address: '{address}'. Error: {e}")
            return None # Unexpected error

    return None # All retries failed

def _extract_text(soup, selector):
    """Helper function to extract text from a CSS selector, returns None if not found."""
    element = soup.select_one(selector)
    return element.text.strip() if element else None

def _extract_relative_text(parent_element, selector):
    """Helper function to extract text from a CSS selector relative to a parent element, returns None if not found."""
    if not parent_element:
        return None
    element = parent_element.select_one(selector)
    return element.text.strip() if element else None


def scrape_property_details(finn_code, config):
    """Scrapes property details for a given Finn code, using the refined Javascript-like method with detailed logging."""
    property_data = {'finn_code': finn_code, 'scrape_status': 'pending'}  # Initialize scrape_status
    ad_url = f"https://www.finn.no/realestate/homes/ad.html?finnkode={finn_code}"
    logger.info(f"Scraping property details from: {ad_url}")
    property_details_selectors = config['property_details_selectors'] # still using config for delays
    scrape_delay_min = config['scrape_delay_min']
    scrape_delay_max = config['scrape_delay_max']

    try:
        response = requests.get(ad_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        # print(response.content) # Print HTML for inspection - keep for debugging

        parser = PropertyParser()
        property_data = parser.parse(soup)

        time.sleep(random.uniform(scrape_delay_min, scrape_delay_max))
        property_data['scrape_status'] = 'success'  # Update status to success if scraping completes

    except requests.exceptions.RequestException as e:
        logger.error(f"Error scraping Finn code {finn_code}: {e}")
        property_data['scrape_status'] = 'failed'  # Update status to failed on error
        return None  # Indicate scraping failed for this finn_code

    return property_data

