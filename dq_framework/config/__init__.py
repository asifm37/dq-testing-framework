"""Configuration module for DQ Framework"""

import json
import os
from typing import Dict, Any


class ConfigManager:
    """Manages configuration loading from schema registry"""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__),
                "schema_registry.json"
            )
        self.config_path = config_path
        self._config = None
    
    @property
    def config(self) -> Dict[str, Any]:
        """Load and cache configuration"""
        if self._config is None:
            with open(self.config_path, 'r') as f:
                self._config = json.load(f)
        return self._config
    
    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """Get configuration for a specific table"""
        tables = self.config.get("tables", {})
        if table_name not in tables:
            raise ValueError(f"Table '{table_name}' not found in schema registry")
        return tables[table_name]
    
    def get_all_tables(self) -> Dict[str, Dict[str, Any]]:
        """Get all table configurations"""
        return self.config.get("tables", {})
    
    def get_global_settings(self) -> Dict[str, Any]:
        """Get global settings"""
        return self.config.get("global_settings", {})
    
    def get_structured_tables(self) -> Dict[str, Dict[str, Any]]:
        """Get only structured table configurations"""
        return {
            name: config 
            for name, config in self.get_all_tables().items()
            if config.get("type") == "structured"
        }
    
    def get_unstructured_tables(self) -> Dict[str, Dict[str, Any]]:
        """Get only unstructured table configurations"""
        return {
            name: config 
            for name, config in self.get_all_tables().items()
            if config.get("type") == "unstructured"
        }


# Singleton instance
_config_manager = None


def get_config_manager() -> ConfigManager:
    """Get singleton ConfigManager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager

