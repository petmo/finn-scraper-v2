import re
import logging
from typing import Optional, Dict, Any, List, Union, Callable
import numpy as np

# Configure logging for production and debugging.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_valid_candidate(candidate: str) -> bool:
    """
    Returns True if the candidate is considered valid.
    Here we simply require the candidate to have more than one character.
    """
    return len(candidate) >= 1


def extract_between_multi(
    text: str,
    start: Union[str, List[str]],
    end: Union[str, List[str]],
    occurrence: int = 1,
    validator: Optional[Callable[[str], bool]] = None,
) -> Optional[str]:
    """
    Searches for substrings in text that occur between any combination
    of start and end tokens (provided as strings or lists of strings).
    It collects all candidate matches (using case-insensitive matching),
    sorts them by their occurrence in the text, filters by the validator if given,
    and returns the nth (occurrence) candidate.
    Returns None if not enough valid candidates are found.
    """
    # Ensure start and end are lists.
    start_list = start if isinstance(start, list) else [start]
    end_list = end if isinstance(end, list) else [end]

    candidates = []
    for s in start_list:
        for e in end_list:
            pattern = re.escape(s) + r"\s*(.*?)\s*(?=" + re.escape(e) + ")"
            for match in re.finditer(pattern, text, re.DOTALL | re.IGNORECASE):
                candidate = match.group(1).strip()
                candidates.append((match.start(), candidate))
    # Sort candidates by their position in the text.
    candidates.sort(key=lambda x: x[0])
    # Filter by validator if provided.
    valid_candidates = [
        cand for pos, cand in candidates if (validator is None or validator(cand))
    ]
    if len(valid_candidates) >= occurrence:
        return valid_candidates[occurrence - 1]
    else:
        logger.debug(
            "No valid candidate found between %s and %s (occurrence %d)",
            start,
            end,
            occurrence,
        )
        return None


class PropertyParser:
    """
    A production-ready parser that extracts structured property information from a text blob.
    This version uses case-insensitive extraction throughout, and splits the text on "nøkkelinfo"
    to limit the search for summary fields. For these fields, it iterates over all matches using
    extract_between_multi() until a valid candidate is found.
    Numeric fields are cleaned by removing spaces and non-digit characters.
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

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalize text by replacing non-breaking spaces and collapsing whitespace.
        """
        text = text.replace("\xa0", " ")
        return re.sub(r"\s+", " ", text).strip()

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

    def post_process_field(self, field: str, value: Optional[str]) -> str:
        """
        Cleans and post-processes an extracted field.
        For numeric fields, extracts only the digits and removes spaces.
        Returns 'not found' if the value is None or empty.
        """
        if value is None or value.strip() == "":
            return np.nan
        if field in self.numeric_fields:
            m = re.search(r"([\d ]+)", value)
            if m:
                return m.group(1).replace(" ", "")
            else:
                logger.warning("Numeric extraction failed for field '%s'", field)
                return np.nan
        else:
            return value.strip()

    def parse_top_section(self, text: str) -> Dict[str, str]:
        """
        Parses the top-level section using top_section_config.
        Returns a dictionary with extracted and cleaned values.
        """
        data = {}
        for field, cfg in self.top_section_config.items():
            extracted = self.extract_field(field, cfg, text)
            data[field] = self.post_process_field(field, extracted)
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
            data[field] = self.post_process_field(field, extracted)
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
        data["rooms"] = self.post_process_field("rooms", rooms_val)
        if data["rooms"] == np.nan:
            logger.warning("Could not extract 'rooms' from text.")
        return data

    def parse(self, soup_obj) -> Dict[str, str]:
        """
        Parses the provided text and returns a dictionary of property information.
        Fields that cannot be found are marked as 'not found'.
        """
        logger.info("Starting parsing process.")
        text = soup_obj.get_text()
        normalized_text = self.normalize_text(text)
        top_data = self.parse_top_section(normalized_text)
        nokkelinfo_data = self.parse_nokkelinfo_fields(normalized_text)
        extract_image_urls = self.extract_all_image_urls(soup_obj)
        local_area = self.extract_local_area(soup_obj)
        breadcrumb_area = self.extract_breadcrumb_area(soup_obj)

        parsed_data = {
            **top_data,
            **nokkelinfo_data,
            **extract_image_urls,
            **local_area,
            **breadcrumb_area,
        }
        logger.info("Parsing complete.")
        return parsed_data

    def extract_local_area(self, soup) -> Dict:
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
            print(f"Error extracting local area: {e}")
            return {"local_area": np.nan}

    def extract_all_image_urls(self, soup, n_images: int = 3) -> Dict:

        image_dict = {}
        for image_id in range(n_images):
            image_url = self.extract_image_url(soup, image_id)
            image_dict[f"image_{image_id}"] = image_url
        return image_dict

    def extract_image_url(self, soup, image_id: int = 0):
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

    def extract_breadcrumb_area(self, soup):
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
            print(f"Error extracting breadcrumb area: {e}")
            return {"area_name": np.nan}
