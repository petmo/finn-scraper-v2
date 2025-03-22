import logging
from typing import Dict, Any, Optional

from scraper.property_details_scraper import scrape_property_details
from scraper.geocoding import geocode_address

logger = logging.getLogger(__name__)

class PropertyService:
    """
    Service for property-related operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the property service.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
    
    def scrape_property(self, finn_code: str) -> Optional[Dict[str, Any]]:
        """
        Scrape property details for a finn code and enrich with geocoding.
        
        Args:
            finn_code: The finn code to scrape
        
        Returns:
            A dictionary with property details, or None if scraping failed
        """
        logger.info(f"Scraping property details for finn code: {finn_code}")
        
        # Scrape property details
        property_data = scrape_property_details(finn_code, self.config)
        
        if property_data:
            property_data["finn_code"] = finn_code  # Ensure finn_code is in property data
            
            # Geocode address
            self._geocode_property(property_data)
            
            return property_data
        else:
            logger.warning(f"Failed to scrape property details for finn code: {finn_code}")
            return None
    
    def _geocode_property(self, property_data: Dict[str, Any]) -> None:
        """
        Geocode the address in property data and add latitude and longitude.
        
        Args:
            property_data: Property data dictionary to update
        """
        address = property_data.get("address")
        
        if not address:
            logger.warning(f"No address found for finn code {property_data.get('finn_code')}, cannot geocode.")
            return
        
        geocode_result = geocode_address(address)
        
        if geocode_result:
            property_data["latitude"] = geocode_result["latitude"]
            property_data["longitude"] = geocode_result["longitude"]
            logger.info(
                f"Geocoded address for finn code {property_data.get('finn_code')}: "
                f"Lat={property_data['latitude']}, Long={property_data['longitude']}"
            )
        else:
            logger.warning(f"Geocoding failed for address: {address} (finn code: {property_data.get('finn_code')})")