import logging
from typing import Dict
from bs4 import BeautifulSoup
import numpy as np
from scraper.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class AreaParser(BaseParser):
    """
    Parses area-related information from the property listing page.
    """

    def extract_area_info(self, soup: BeautifulSoup) -> Dict:
        area_data = {}
        area_data.update(self.extract_local_area(soup))
        area_data.update(self.extract_breadcrumb_area(soup))
        return area_data

    def extract_local_area(self, soup: BeautifulSoup) -> Dict:
        """
        Extracts the local area name from the property listing page, handling different structures.
        """
        try:
            # Attempt to find by `data-testid`
            local_area_tag = soup.find("div", {"data-testid": "local-area-name"})
            if local_area_tag:
                return {"local_area": local_area_tag.text.strip().lower()}

            # If not found, try alternative search inside sections
            sections = soup.find_all("section")
            for section in sections:
                local_area_tag = section.find("div", {"data-testid": "local-area-name"})
                if local_area_tag:
                    return {"local_area": local_area_tag.text.strip().lower()}

            return {"local_area": np.nan}
        except Exception as e:
            logger.error(f"Error extracting local area: {e}")
            return {"local_area": np.nan}

    def extract_breadcrumb_area(self, soup: BeautifulSoup) -> Dict:
        """
        Extracts the breadcrumb area name from the navigation bar.
        It selects the 4th <a> tag inside the breadcrumb <nav>.
        """
        try:
            breadcrumb_nav = soup.find("nav")  # Locate the breadcrumb navigation
            if breadcrumb_nav:
                breadcrumb_links = breadcrumb_nav.find_all(
                    "a"
                )  # Get all <a> links inside <nav>
                if len(breadcrumb_links) >= 4:
                    return {
                        "area_name": breadcrumb_links[3].text.strip()
                    }  # 4th link (index 3 in Python)

            return {"area_name": np.nan}  # Return NaN if not found
        except Exception as e:
            logger.error(f"Error extracting breadcrumb area: {e}")
            return {"area_name": np.nan}
