"""
Centralized configuration manager to avoid multiple Config instances.
"""
from utils import Config

# Global config instance - loaded once
_config_instance = None

def get_config() -> Config:
    """Get the global config instance, creating it only once."""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance

def refresh_config():
    """Force a refresh of the global config instance."""
    global _config_instance
    _config_instance = None
    return get_config()
