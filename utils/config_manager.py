# -*- coding: utf-8 -*-
"""
Author: Paras Parkash
Source: Market Data Acquisition System
Configuration management module
"""
import json
import os
from typing import Any, Dict
import threading

class ConfigManager:
    """
    Singleton configuration manager to handle system configuration
    """
    _instance = None
    _config = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def load_config(self, config_path: str = 'market_data_config.json') -> Dict[str, Any]:
        """
        Load configuration from file
        """
        if self._config is None:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Configuration file {config_path} not found")
            
            with open(config_path, 'r') as config_file:
                self._config = json.load(config_file)
        return self._config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key
        """
        if self._config is None:
            self.load_config()
        return self._config.get(key, default)
    
    def get_required(self, key: str) -> Any:
        """
        Get required configuration value by key, raise exception if not found
        """
        if self._config is None:
            self.load_config()
        if key not in self._config:
            raise KeyError(f"Required configuration key '{key}' not found")
        return self._config[key]
    
    def is_default_value(self, key: str) -> bool:
        """
        Check if a configuration value is a default placeholder
        """
        if self._config is None:
            self.load_config()
        
        value = self._config.get(key)
        if value is None:
            return True
            
        # Check for common default placeholders
        default_placeholders = [
            "your_notification_recipients",
            "your_email_sender@gmail.com", 
            "your_email_password",
            "your_totp_secret",
            "your_broker_username",
            "your_broker_password", 
            "your_api_key",
            "your_api_secret",
            "your_database_host",
            "your_database_user",
            "your_database_password"
        ]
        
        return str(value) in default_placeholders

# Global configuration manager instance
config_manager = ConfigManager()