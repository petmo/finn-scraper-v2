# finn_scraper/scraper/utils.py
import yaml
import logging.config
import re
import logging
from typing import Optional, Union, List, Callable

logger = logging.getLogger(__name__)


def load_config(config_path="config/config.yaml"):
    """Loads configuration from YAML file."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def setup_logging(config_path="config/logging.yaml"):
    """Sets up logging configuration from YAML file."""
    with open(config_path, "r") as file:
        log_config = yaml.safe_load(file)
        logging.config.dictConfig(log_config)


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
