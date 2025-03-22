from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    Defines the interface that all storage backends must implement.
    """

    @abstractmethod
    def initialize(self) -> None:
        """Initialize the storage backend (create tables, directories, etc.)"""
        pass

    @abstractmethod
    def save_finn_codes(self, finn_codes_data: List[Dict[str, Any]]) -> None:
        """Save finn codes to the storage backend"""
        pass

    @abstractmethod
    def fetch_finn_codes(self, select_all: bool = False) -> List[tuple]:
        """Fetch finn codes from the storage backend"""
        pass

    @abstractmethod
    def save_property_data(self, property_data: Dict[str, Any]) -> None:
        """Save property data to the storage backend"""
        pass

    @abstractmethod
    def update_finn_code_status(self, finn_code: str, status: str) -> None:
        """Update the scrape status of a finn code"""
        pass

    @abstractmethod
    def export_to_csv(self, csv_name: str) -> None:
        """Export data to a CSV file"""
        pass

    @abstractmethod
    def export_finn_codes_to_csv(self, csv_name: str) -> None:
        """Export finn codes to a CSV file"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the storage backend (close connections, etc.)"""
        pass

    @abstractmethod
    def update_finn_code_status(self, finn_code: str, status: str) -> None:
        """Update the scrape status of a finn code"""
        pass

    @abstractmethod
    def update_finn_code_status_fields(self, finn_code: str, **kwargs) -> None:
        """Update multiple status fields of a finn code"""
        pass

    @abstractmethod
    def fetch_finn_codes_with_status(self, status: str = None) -> List[tuple]:
        """Fetch finn codes with a specific listing status (active/inactive)"""
        pass

    @abstractmethod
    def property_exists(self, finn_code: str) -> bool:
        """Check if a property exists in the properties table"""
        pass

    @abstractmethod
    def fetch_properties(
        self, finn_codes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch properties from storage, optionally filtered by finn codes"""
        pass
