from typing import Union

from bs4 import BeautifulSoup


def extract_text(soup: BeautifulSoup, selector: str) -> Union[str, None]:
    """Helper function to extract text from a CSS selector, returns None if not found."""
    element = soup.select_one(selector)
    return element.text.strip() if element else None


def extract_relative_text(
    parent_element: BeautifulSoup, selector: str
) -> Union[str, None]:
    """Helper function to extract text from a CSS selector relative to a parent element, returns None if not found."""
    if not parent_element:
        return None
    element = parent_element.select_one(selector)
    return element.text.strip() if element else None
