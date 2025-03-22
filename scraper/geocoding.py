import time
import logging

from geopy import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError


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
    geolocator = Nominatim(user_agent="finn_property_scraper")  # Set a user agent

    for attempt in range(retries):
        try:
            location = geolocator.geocode(address, timeout=10)  # Geocode with timeout
            if location:
                return {"latitude": location.latitude, "longitude": location.longitude}
            else:
                logger.warning(
                    f"Geocoding failed for address: '{address}'. No location found by Nominatim."
                )
                return None  # Geocoding failed, no location found

        except GeocoderTimedOut:
            if attempt < retries - 1:
                logger.warning(
                    f"Geocoding timed out for address: '{address}'. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"Geocoding timed out for address: '{address}' after {retries} retries."
                )
                return None  # Geocoding timed out after retries

        except GeocoderServiceError as e:
            if attempt < retries - 1 and e.status_code == 429:  # Rate limit error (429)
                retry_delay = delay * (attempt + 1)  # Exponential backoff
                logger.warning(
                    f"Geocoding service error (likely rate limited) for address: '{address}'. Retrying in {retry_delay} seconds..."
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    f"Geocoding service error for address: '{address}'. Error: {e}"
                )
                return None  # Geocoding service error

        except Exception as e:
            logger.error(
                f"An unexpected error occurred during geocoding for address: '{address}'. Error: {e}"
            )
            return None  # Unexpected error

    return None  # All retries failed
