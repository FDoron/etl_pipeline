import os
import yaml

class Config:
    _config = None

    @classmethod
    def load(cls, config_file: str = "config/settings.yaml"):
        """
        Load the YAML configuration into memory (singleton style).
        """
        if cls._config is None:
            if not os.path.exists(config_file):
                raise FileNotFoundError(f"Config file not found: {config_file}")
            with open(config_file, "r", encoding="utf-8") as f:
                cls._config = yaml.safe_load(f)
        return cls._config

    @classmethod
    def get(cls, key_path: str, default=None):
        """
        Retrieve nested config values using dot notation, e.g., 'db.host'.
        """
        if cls._config is None:
            raise RuntimeError("Config not loaded. Call Config.load() first.")

        keys = key_path.split(".")
        value = cls._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value
