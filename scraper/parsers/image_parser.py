import logging
from typing import Dict
from bs4 import BeautifulSoup
import numpy as np
from scraper.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class ImageParser(BaseParser):
    """
    Parses image-related information from the property listing page.
    """

    def extract_image_urls(self, soup: BeautifulSoup, n_images: int = 3) -> Dict:
        image_dict = {}
        for image_id in range(n_images):
            image_url = self.extract_image_url(soup, image_id)
            image_dict[f"image_{image_id}"] = image_url
        return image_dict

    def extract_image_url(self, soup: BeautifulSoup, image_id: int = 0):
        """
        Extracts the highest quality image URL from the 'srcset' attribute of the image with id="image-0".
        """
        try:
            img_tag = soup.find("img", {"id": f"image-{image_id}"})
            if img_tag:
                srcset = img_tag.get("srcset") or img_tag.get("data-srcset")
                if srcset:
                    # Split the srcset and get the highest resolution image (first entry)
                    image_url = srcset.split(",")[0].split(" ")[0].strip()
                    return image_url
                else:
                    # If no srcset, try to get the regular src attribute
                    return img_tag.get("src")
            logger.warning(f"No image found for image id {image_id}")
            return np.nan
        except Exception as e:
            logger.warning(f"Error extracting image URL: {e}")
            return np.nan
