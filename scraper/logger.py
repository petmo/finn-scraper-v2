"""
Enhanced logging configuration module.
"""

import os
import logging
import logging.config
import yaml
from typing import Optional, Dict, Any

DEFAULT_LOG_CONFIG_PATH = "config/logging.yaml"


def setup_logging(
    config_path: Optional[str] = None,
    default_level: int = logging.INFO,
    env_key: str = "LOG_CONFIG",
) -> None:
    """
    Set up logging configuration from a YAML file.

    Args:
        config_path: Path to the logging config file
        default_level: Default logging level if config file not found
        env_key: Environment variable that can specify the config path
    """
    path = config_path or os.getenv(env_key, DEFAULT_LOG_CONFIG_PATH)

    if os.path.exists(path):
        with open(path, "rt") as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
                return
            except Exception as e:
                print(f"Error loading logging configuration: {e}")
                print(f"Falling back to basic configuration (level={default_level})")

    # Fallback to basic configuration
    logging.basicConfig(
        level=default_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


class LoggerAdapter(logging.LoggerAdapter):
    """
    Enhanced logger adapter with context.

    This adapter allows adding context info to log messages.
    """

    def __init__(self, logger: logging.Logger, context: Dict[str, Any] = None):
        """
        Initialize the adapter with a logger and context.

        Args:
            logger: Base logger instance
            context: Context dictionary to include in log messages
        """
        super().__init__(logger, context or {})

    def process(self, msg, kwargs):
        """Process the log message to add context."""
        context_str = " ".join(f"{k}={v}" for k, v in self.extra.items())
        if context_str:
            msg = f"{msg} [{context_str}]"
        return msg, kwargs

    def update_context(self, **kwargs) -> None:
        """
        Update the context dictionary.

        Args:
            **kwargs: Key-value pairs to add to context
        """
        self.extra.update(kwargs)

    def clear_context(self) -> None:
        """Clear the context dictionary."""
        self.extra.clear()
