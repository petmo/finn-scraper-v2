import logging
import datetime
from typing import List, Dict, Any, Set

from scraper.finn_code_scraper import fetch_finn_codes

logger = logging.getLogger(__name__)

class FinnCodeService:
    """
    Service for finn code-related operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the finn code service.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
    
    def fetch_finn_codes(self) -> List[Dict[str, Any]]:
        """
        Fetch finn codes from Finn.no.
        
        Returns:
            A list of dictionaries with finn code data
        """
        logger.info("Fetching finn codes from Finn.no")
        
        # Fetch finn codes
        finn_codes: Set[str] = fetch_finn_codes(self.config)
        
        # Convert to list of dictionaries
        finn_codes_data = []
        for finn_code in finn_codes:
            finn_codes_data.append({
                "finn_code": finn_code,
                "fetched_at": datetime.datetime.now().isoformat(),
                "scrape_status": "pending"
            })
        
        logger.info(f"Fetched {len(finn_codes_data)} finn codes from Finn.no")
        
        return finn_codes_data