
# finn_scraper/scraper/utils.py
import yaml
import logging.config

def load_config(config_path="config/config.yaml"):
    """Loads configuration from YAML file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def setup_logging(config_path="config/logging.yaml"):
    """Sets up logging configuration from YAML file."""
    with open(config_path, 'r') as file:
        log_config = yaml.safe_load(file)
        logging.config.dictConfig(log_config)
