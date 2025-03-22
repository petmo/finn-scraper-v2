import os
import yaml
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Enhanced configuration manager with environment variable support.
    """

    DEFAULT_CONFIG_PATH = "config/config.yaml"
    ENV_PREFIX = "FINN_"

    _instance = None

    def __new__(cls, config_path: Optional[str] = None):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._config = None
            cls._instance._config_path = config_path or cls.DEFAULT_CONFIG_PATH
        return cls._instance

    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file and override with environment variables.

        Returns:
            Configuration dictionary
        """
        if self._config is not None:
            return self._config

        try:
            with open(self._config_path, "r") as file:
                self._config = yaml.safe_load(file)
                logger.info(f"Configuration loaded from {self._config_path}")

            # Override with environment variables
            self._override_from_env()

            return self._config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    def _override_from_env(self) -> None:
        """Override configuration values from environment variables."""
        for env_var, value in os.environ.items():
            if env_var.startswith(self.ENV_PREFIX):
                # Convert FINN_SUPABASE_URL to config["supabase"]["url"]
                path = env_var[len(self.ENV_PREFIX) :].lower().split("_")

                # Navigate to the correct config location
                config_section = self._config
                for i, key in enumerate(path):
                    if i == len(path) - 1:
                        # Last element is the actual key to set
                        config_section[key] = value
                        logger.debug(
                            f"Config override from env: {env_var} -> {'.'.join(path)}"
                        )
                    else:
                        # Create nested dict if needed
                        if key not in config_section:
                            config_section[key] = {}
                        config_section = config_section[key]

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.

        Args:
            key: Configuration key (dot notation for nested keys)
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        if self._config is None:
            self.load_config()

        # Handle nested keys with dot notation (e.g., "supabase.url")
        keys = key.split(".")
        value = self._config

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.

        Args:
            key: Configuration key (dot notation for nested keys)
            value: Value to set
        """
        if self._config is None:
            self.load_config()

        keys = key.split(".")
        config_section = self._config

        # Navigate to the correct location
        for i, k in enumerate(keys):
            if i == len(keys) - 1:
                # Last key, set the value
                config_section[k] = value
            else:
                # Create nested dict if needed
                if k not in config_section:
                    config_section[k] = {}
                config_section = config_section[k]

    def validate_required_keys(self, *keys: str) -> bool:
        """
        Check if required keys exist in the configuration.

        Args:
            keys: Required configuration keys

        Returns:
            True if all keys exist and have non-empty values
        """
        if self._config is None:
            self.load_config()

        for key in keys:
            value = self.get(key)
            if value is None or (isinstance(value, str) and not value):
                logger.error(f"Required configuration key missing or empty: {key}")
                return False

        return True
