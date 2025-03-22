import re
import logging
from abc import abstractmethod
from typing import Dict, Any, Optional, Union, List, Callable, TypeVar, Generic
import numpy as np

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BaseParser(Generic[T]):
    """
    Enhanced base class for all parsers with generics support.

    This defines the interface and common utilities for all parsers in the system.
    """

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalize text by replacing non-breaking spaces and collapsing whitespace.

        Args:
            text: The text to normalize

        Returns:
            Normalized text with consistent whitespace
        """
        text = text.replace("\xa0", " ")
        return re.sub(r"\s+", " ", text).strip()

    def post_process_field(
        self, field: str, value: Optional[str], numeric_fields: set = None
    ) -> Any:
        """
        Cleans and post-processes an extracted field.

        For numeric fields, extracts only the digits and removes spaces.
        Returns np.nan if the value is None or empty.

        Args:
            field: The field name
            value: The extracted value
            numeric_fields: Set of field names that should be treated as numeric

        Returns:
            Post-processed value (str, numeric, or np.nan)
        """
        if value is None or value.strip() == "":
            return np.nan

        if numeric_fields and field in numeric_fields:
            m = re.search(r"([\d ]+)", value)
            if m:
                return m.group(1).replace(" ", "")
            else:
                logger.warning(
                    f"Numeric extraction failed for field '{field}' with value '{value}'"
                )
                return np.nan
        else:
            return value.strip()

    def extract_with_pattern(
        self, field: str, pattern: str, text: str, fallback_patterns: List[str] = None
    ) -> Optional[str]:
        """
        Extract a field using regex patterns with fallbacks.

        Args:
            field: Field name for logging
            pattern: Primary regex pattern
            text: Text to search
            fallback_patterns: List of fallback patterns to try if primary fails

        Returns:
            Extracted text or None if no match found
        """
        patterns = [pattern]
        if fallback_patterns:
            patterns.extend(fallback_patterns)

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

    def extract_between(
        self,
        text: str,
        start: Union[str, List[str]],
        end: Union[str, List[str]],
        occurrence: int = 1,
        validator: Optional[Callable[[str], bool]] = None,
    ) -> Optional[str]:
        """
        Extract text between start and end delimiters.

        Args:
            text: Text to search in
            start: Start delimiter(s)
            end: End delimiter(s)
            occurrence: Which occurrence to return (1-based)
            validator: Function to validate candidate matches

        Returns:
            Extracted text or None if no valid match
        """
        # Ensure start and end are lists
        start_list = start if isinstance(start, list) else [start]
        end_list = end if isinstance(end, list) else [end]

        candidates = []
        for s in start_list:
            for e in end_list:
                pattern = re.escape(s) + r"\s*(.*?)\s*(?=" + re.escape(e) + ")"
                for match in re.finditer(pattern, text, re.DOTALL | re.IGNORECASE):
                    candidate = match.group(1).strip()
                    candidates.append((match.start(), candidate))

        # Sort candidates by their position in the text
        candidates.sort(key=lambda x: x[0])

        # Filter by validator if provided
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

    @abstractmethod
    def parse(self, data: Any) -> T:
        """
        Parse input data and return structured output.

        Args:
            data: The input data to parse

        Returns:
            Parsed data structure of type T
        """
        pass
