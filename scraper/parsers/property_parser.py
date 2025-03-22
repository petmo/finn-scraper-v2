import re
import logging
from typing import Optional, Dict, Any, List
import numpy as np
from bs4 import BeautifulSoup

from scraper.parsers.area_parser import AreaParser
from scraper.parsers.image_parser import ImageParser
from scraper.utils import extract_between_multi, is_valid_candidate
from scraper.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class PropertyParser(BaseParser):
    """
    Parses structured property information from a text blob.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        # Configuration for top-level fields extraction.
        self.top_section_config = {
            "title": {
                "pattern": r"^(.*?)\s*\|\s*finn eiendom",
                "fallback_patterns": [r"^(.*?)\s*\|"],
                "description": "Extracts the title from text before '| FINN eiendom'.",
            },
            "address": {
                "pattern": r"kart med kartnål\s*(.*?)\s*prisantydning",
                "fallback_patterns": [r"(osterhaus\' gate.*?\d{4}\s*oslo)"],
                "description": "Extracts the address between 'kart med kartnål' and 'prisantydning'.",
            },
            "asking_price": {
                "pattern": r"prisantydning\s*([\d ]+ ?kr)",
                "description": "Extracts the price suggestion (list price).",
            },
            "total_price": {
                "pattern": r"totalpris\s*([\d ]+ ?kr)",
                "description": "Extracts the total price.",
            },
            "costs": {
                "pattern": r"omkostninger\s*([\d ]+ ?kr)",
                "description": "Extracts the costs.",
            },
            "joint_debt": {
                "pattern": r"fellesgjeld\s*([\d ]+ ?kr)",
                "description": "Extracts the joint debt.",
            },
            "monthly_fee": {
                "pattern": r"felleskost/mnd\.\s*([\d ]+ ?kr)",
                "description": "Extracts the monthly fee.",
            },
        }

        # Configuration for summary fields (nøkkelinfo).
        # These delimiters will be searched only in the text after the "nøkkelinfo" keyword.
        # Now both "start" and "end" can be lists.
        self.nokkelinfo_config = {
            "property_type": {"start": "boligtype", "end": ["eieform"]},
            "ownership": {"start": "eieform", "end": ["soverom", "internt bruksareal"]},
            "bedrooms": {"start": "soverom", "end": ["internt bruksareal"]},
            "internal_area": {"start": "internt bruksareal", "end": ["bruksareal"]},
            # For usable_area, we assume that the desired candidate is found between a token (e.g. "(bra-i)bruksareal")
            # and an end token list (e.g. ["eksternt bruksareal"]). Adjust as needed.
            "usable_area": {
                "start": ["(bra-i)bruksareal"],
                "end": ["eksternt bruksareal", "balkong"],
            },
            "external_usable_area": {
                "start": ["eksternt bruksareal"],
                "end": ["(bra-e)"],
            },
            "floor": {"start": "etasje", "end": ["byggeår"]},
            "build_year": {"start": "byggeår", "end": ["energimerking", "rom"]},
        }

        # List of fields expected to be numeric.
        self.numeric_fields = {
            "asking_price",
            "total_price",
            "costs",
            "joint_debt",
            "monthly_fee",
            "bedrooms",
            "internal_area",
            "usable_area",
            "external_usable_area",
            "floor",
            "build_year",
            "rooms",
        }

        # Allow external configuration to override defaults.
        if config is not None:
            self.top_section_config.update(config.get("top_section_config", {}))
            self.nokkelinfo_config.update(config.get("nokkelinfo_config", {}))

    def extract_field(
        self, field: str, cfg: Dict[str, Any], text: str
    ) -> Optional[str]:
        """
        Attempt extraction using the primary pattern and fallback patterns (all case-insensitive).
        """
        patterns: List[str] = []
        if "pattern" in cfg and cfg["pattern"]:
            patterns.append(cfg["pattern"])
        if "fallback_patterns" in cfg:
            patterns.extend(cfg["fallback_patterns"])
        for pat in patterns:
            try:
                match = re.search(pat, text, re.DOTALL | re.IGNORECASE)
                if match:
                    logger.debug("Field '%s' extracted using pattern: %s", field, pat)
                    return match.group(1).strip()
            except Exception as e:
                logger.exception(
                    "Error processing field '%s' with pattern '%s': %s", field, pat, e
                )
        logger.warning("Pattern not found for field '%s'", field)
        return None

    def extract_nokkelinfo_field(
        self, field: str, cfg: Dict[str, Any], text: str
    ) -> Optional[str]:
        """
        Uses extract_between_multi() to search for a field using the provided start and end tokens
        (which can be either strings or lists), and returns the first valid candidate.
        """
        start_token = cfg["start"]
        end_token = cfg["end"]
        # Optionally, you can add an occurrence parameter here if needed:
        return extract_between_multi(
            text, start_token, end_token, occurrence=1, validator=is_valid_candidate
        )

    def parse_top_section(self, text: str) -> Dict[str, str]:
        """
        Parses the top-level section using top_section_config.
        Returns a dictionary with extracted and cleaned values.
        """
        data = {}
        for field, cfg in self.top_section_config.items():
            extracted = self.extract_field(field, cfg, text)
            data[field] = self.post_process_field(field, extracted, self.numeric_fields)
        return data

    def parse_nokkelinfo_fields(self, text: str) -> Dict[str, str]:
        """
        Splits the text on "nøkkelinfo" (case-insensitive) and parses the fields defined in nokkelinfo_config
        from the portion after that split. Also attempts to extract 'rooms' using a global regex.
        """
        data = {}
        parts = re.split(r"nøkkelinfo", text, flags=re.IGNORECASE)
        nokkelinfo_text = parts[1] if len(parts) > 1 else text
        for field, cfg in self.nokkelinfo_config.items():
            extracted = self.extract_nokkelinfo_field(field, cfg, nokkelinfo_text)
            data[field] = self.post_process_field(field, extracted, self.numeric_fields)
            if data[field] == np.nan:
                logger.warning(
                    "Field '%s' could not be extracted using start %s and end %s",
                    field,
                    cfg["start"],
                    cfg["end"],
                )

        # Extract 'rooms' globally.
        m = re.search(r"rom\s*(\d+)", text, re.IGNORECASE)
        rooms_val = m.group(1).strip() if m else None
        data["rooms"] = self.post_process_field("rooms", rooms_val, self.numeric_fields)
        if data["rooms"] == np.nan:
            logger.warning("Could not extract 'rooms' from text.")
        return data

    def parse(self, soup_obj: BeautifulSoup) -> Dict[str, str]:
        """
        Parses the provided BeautifulSoup object and returns a dictionary of property information.
        Fields that cannot be found are marked as np.nan.
        """
        logger.info("Starting parsing process.")
        text = soup_obj.get_text()
        normalized_text = self.normalize_text(text)
        top_data = self.parse_top_section(normalized_text)
        nokkelinfo_data = self.parse_nokkelinfo_fields(normalized_text)

        area_parser = AreaParser()
        area_data = area_parser.extract_area_info(soup_obj)

        image_parser = ImageParser()
        image_data = image_parser.extract_image_urls(soup_obj)

        parsed_data = {
            **top_data,
            **nokkelinfo_data,
            **area_data,
            **image_data,
        }
        logger.info("Parsing complete.")
        return parsed_data
