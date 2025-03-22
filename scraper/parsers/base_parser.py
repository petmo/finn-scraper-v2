import re
import logging
from typing import Dict, Any, Optional
import numpy as np

logger = logging.getLogger(__name__)


class BaseParser:
    """
    Base class for parsers, providing common utility methods.
    """

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalize text by replacing non-breaking spaces and collapsing whitespace.
        """
        text = text.replace("\xa0", " ")
        return re.sub(r"\s+", " ", text).strip()

    def post_process_field(
        self, field: str, value: Optional[str], numeric_fields: set = None
    ) -> str:
        """
        Cleans and post-processes an extracted field.
        For numeric fields, extracts only the digits and removes spaces.
        Returns np.nan if the value is None or empty.
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
