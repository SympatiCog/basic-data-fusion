"""
Configuration management for Basic Data Fusion.

This module provides a split configuration system that separates concerns
into focused configuration classes while maintaining backward compatibility.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import toml

from .exceptions import ConfigurationError


@dataclass
class DataConfig:
    """Configuration for data handling and file operations."""
    
    # File and directory settings
    data_dir: str = 'data'
    demographics_file: str = 'demographics.csv'
    
    # Column name mappings
    primary_id_column: str = 'ursi'
    session_column: str = 'session_num'
    composite_id_column: str = 'customID'
    age_column: str = 'age'
    sex_column: str = 'sex'
    study_site_column: Optional[str] = None
    
    # Rockland Study Configuration
    rockland_base_studies: List[str] = field(default_factory=lambda: [
        'Discovery', 'Longitudinal_Adult', 'Longitudinal_Child', 'Neurofeedback'
    ])
    default_rockland_studies: List[str] = field(default_factory=lambda: [
        'Discovery', 'Longitudinal_Adult', 'Longitudinal_Child', 'Neurofeedback'
    ])
    rockland_sample1_columns: List[str] = field(default_factory=lambda: ['rockland-sample1'])
    rockland_sample1_labels: Dict[str, str] = field(default_factory=lambda: {
        'rockland-sample1': 'Rockland Sample 1'
    })
    default_rockland_sample1_selection: List[str] = field(default_factory=lambda: ['rockland-sample1'])
    
    def get_demographics_table_name(self) -> str:
        """Get the demographics table name without extension."""
        return Path(self.demographics_file).stem
    
    def get_demographics_path(self) -> str:
        """Get the full path to the demographics file."""
        return os.path.join(self.data_dir, self.demographics_file)
    
    def validate(self) -> List[str]:
        """Validate the data configuration and return any errors."""
        errors = []
        
        if not self.data_dir:
            errors.append("data_dir cannot be empty")
        
        if not self.demographics_file:
            errors.append("demographics_file cannot be empty")
        
        if not self.primary_id_column:
            errors.append("primary_id_column cannot be empty")
        
        return errors


@dataclass
class UIConfig:
    """Configuration for user interface settings."""
    
    # Display defaults
    default_age_range: Tuple[int, int] = (0, 120)
    default_age_selection: Tuple[int, int] = (18, 80)
    default_filter_range: Tuple[int, int] = (0, 100)
    max_display_rows: int = 50
    
    # Cache settings
    cache_ttl_seconds: int = 600
    
    def validate(self) -> List[str]:
        """Validate the UI configuration and return any errors."""
        errors = []
        
        if self.default_age_range[0] >= self.default_age_range[1]:
            errors.append("default_age_range min must be less than max")
        
        if self.default_age_selection[0] >= self.default_age_selection[1]:
            errors.append("default_age_selection min must be less than max")
        
        if self.max_display_rows <= 0:
            errors.append("max_display_rows must be positive")
        
        if self.cache_ttl_seconds <= 0:
            errors.append("cache_ttl_seconds must be positive")
        
        return errors


@dataclass
class StateConfig:
    """Configuration for state management."""
    
    # State management settings
    backend: str = 'client'  # 'client', 'memory', 'redis', 'database'
    ttl_default: int = 3600  # 1 hour default TTL
    enable_user_isolation: bool = True
    
    # Backend-specific settings
    redis_url: str = 'redis://localhost:6379/0'
    database_url: str = 'sqlite:///state.db'
    
    def validate(self) -> List[str]:
        """Validate the state configuration and return any errors."""
        errors = []
        
        valid_backends = ['client', 'memory', 'redis', 'database']
        if self.backend not in valid_backends:
            errors.append(f"backend must be one of {valid_backends}")
        
        if self.ttl_default <= 0:
            errors.append("ttl_default must be positive")
        
        return errors


@dataclass
class Config:
    """Main configuration class that combines all configuration sections."""
    
    config_file_path: str = "config.toml"
    
    # Configuration sections
    data: DataConfig = field(default_factory=DataConfig)
    ui: UIConfig = field(default_factory=UIConfig)
    state: StateConfig = field(default_factory=StateConfig)
    
    # Private fields for merge strategy (maintained for backward compatibility)
    _merge_strategy: Optional[object] = field(init=False, default=None, repr=False)
    _merge_keys: Optional[object] = field(init=False, default=None, repr=False)
    
    def __post_init__(self):
        """Load configuration when an instance is created."""
        self.load_config()
    
    def save_config(self) -> None:
        """Save current configuration to TOML file."""
        config_data = {
            'data': {
                'data_dir': self.data.data_dir,
                'demographics_file': self.data.demographics_file,
                'primary_id_column': self.data.primary_id_column,
                'session_column': self.data.session_column,
                'composite_id_column': self.data.composite_id_column,
                'age_column': self.data.age_column,
                'sex_column': self.data.sex_column,
                'study_site_column': self.data.study_site_column,
            },
            'ui': {
                'default_age_min': self.ui.default_age_selection[0],
                'default_age_max': self.ui.default_age_selection[1],
                'max_display_rows': self.ui.max_display_rows,
            },
            'state': {
                'backend': self.state.backend,
                'ttl_default': self.state.ttl_default,
                'enable_user_isolation': self.state.enable_user_isolation,
            }
        }
        
        try:
            with open(self.config_file_path, 'w') as f:
                toml.dump(config_data, f)
            logging.info(f"Configuration saved to {self.config_file_path}")
        except Exception as e:
            error_msg = f"Error saving configuration: {e}"
            logging.error(error_msg)
            raise ConfigurationError(error_msg, config_file=self.config_file_path)
    
    def load_config(self) -> None:
        """Load configuration from TOML file."""
        try:
            with open(self.config_file_path) as f:
                config_data = toml.load(f)
            
            # Load data configuration
            if 'data' in config_data:
                data_config = config_data['data']
                self.data.data_dir = data_config.get('data_dir', self.data.data_dir)
                self.data.demographics_file = data_config.get('demographics_file', self.data.demographics_file)
                self.data.primary_id_column = data_config.get('primary_id_column', self.data.primary_id_column)
                self.data.session_column = data_config.get('session_column', self.data.session_column)
                self.data.composite_id_column = data_config.get('composite_id_column', self.data.composite_id_column)
                self.data.age_column = data_config.get('age_column', self.data.age_column)
                self.data.sex_column = data_config.get('sex_column', self.data.sex_column)
                self.data.study_site_column = data_config.get('study_site_column', self.data.study_site_column)
            
            # Load UI configuration
            if 'ui' in config_data:
                ui_config = config_data['ui']
                default_age_min = ui_config.get('default_age_min', self.ui.default_age_selection[0])
                default_age_max = ui_config.get('default_age_max', self.ui.default_age_selection[1])
                self.ui.default_age_selection = (default_age_min, default_age_max)
                self.ui.max_display_rows = ui_config.get('max_display_rows', self.ui.max_display_rows)
            
            # Load state configuration
            if 'state' in config_data:
                state_config = config_data['state']
                self.state.backend = state_config.get('backend', self.state.backend)
                self.state.ttl_default = state_config.get('ttl_default', self.state.ttl_default)
                self.state.enable_user_isolation = state_config.get('enable_user_isolation', self.state.enable_user_isolation)
            
            logging.info(f"Configuration loaded from {self.config_file_path}")
            self.refresh_merge_detection()
        
        except FileNotFoundError:
            logging.info(f"{self.config_file_path} not found. Creating with default values.")
            self.save_config()
        except toml.TomlDecodeError as e:
            error_msg = f"Error decoding {self.config_file_path}: {e}. Using default configuration."
            logging.error(error_msg)
            raise ConfigurationError(error_msg, config_file=self.config_file_path)
        except Exception as e:
            error_msg = f"Error loading configuration: {e}. Using default configuration."
            logging.error(error_msg)
            raise ConfigurationError(error_msg, config_file=self.config_file_path)
    
    def validate(self) -> List[str]:
        """Validate all configuration sections and return any errors."""
        errors = []
        errors.extend(self.data.validate())
        errors.extend(self.ui.validate())
        errors.extend(self.state.validate())
        return errors
    
    def refresh_merge_detection(self) -> None:
        """Refresh merge strategy detection (maintained for backward compatibility)."""
        self._merge_keys = None
        self._merge_strategy = None
        # Note: Actual merge strategy initialization will be handled by data processing module
    
    # Backward compatibility properties
    @property
    def DATA_DIR(self) -> str:
        """Backward compatibility property."""
        return self.data.data_dir
    
    @DATA_DIR.setter
    def DATA_DIR(self, value: str) -> None:
        """Backward compatibility property setter."""
        self.data.data_dir = value
    
    @property
    def DEMOGRAPHICS_FILE(self) -> str:
        """Backward compatibility property."""
        return self.data.demographics_file
    
    @DEMOGRAPHICS_FILE.setter
    def DEMOGRAPHICS_FILE(self, value: str) -> None:
        """Backward compatibility property setter."""
        self.data.demographics_file = value
    
    @property
    def PRIMARY_ID_COLUMN(self) -> str:
        """Backward compatibility property."""
        return self.data.primary_id_column
    
    @PRIMARY_ID_COLUMN.setter
    def PRIMARY_ID_COLUMN(self, value: str) -> None:
        """Backward compatibility property setter."""
        self.data.primary_id_column = value
    
    @property
    def SESSION_COLUMN(self) -> str:
        """Backward compatibility property."""
        return self.data.session_column
    
    @SESSION_COLUMN.setter
    def SESSION_COLUMN(self, value: str) -> None:
        """Backward compatibility property setter."""
        self.data.session_column = value
    
    @property
    def COMPOSITE_ID_COLUMN(self) -> str:
        """Backward compatibility property."""
        return self.data.composite_id_column
    
    @COMPOSITE_ID_COLUMN.setter
    def COMPOSITE_ID_COLUMN(self, value: str) -> None:
        """Backward compatibility property setter."""
        self.data.composite_id_column = value
    
    @property
    def AGE_COLUMN(self) -> str:
        """Backward compatibility property."""
        return self.data.age_column
    
    @AGE_COLUMN.setter
    def AGE_COLUMN(self, value: str) -> None:
        """Backward compatibility property setter."""
        self.data.age_column = value
    
    @property
    def SEX_COLUMN(self) -> str:
        """Backward compatibility property."""
        return self.data.sex_column
    
    @SEX_COLUMN.setter
    def SEX_COLUMN(self, value: str) -> None:
        """Backward compatibility property setter."""
        self.data.sex_column = value
    
    @property
    def DEFAULT_AGE_SELECTION(self) -> Tuple[int, int]:
        """Backward compatibility property."""
        return self.ui.default_age_selection
    
    @DEFAULT_AGE_SELECTION.setter
    def DEFAULT_AGE_SELECTION(self, value: Tuple[int, int]) -> None:
        """Backward compatibility property setter."""
        self.ui.default_age_selection = value
    
    @property
    def MAX_DISPLAY_ROWS(self) -> int:
        """Backward compatibility property."""
        return self.ui.max_display_rows
    
    @MAX_DISPLAY_ROWS.setter
    def MAX_DISPLAY_ROWS(self, value: int) -> None:
        """Backward compatibility property setter."""
        self.ui.max_display_rows = value
    
    def get_demographics_table_name(self) -> str:
        """Get the demographics table name without extension."""
        return self.data.get_demographics_table_name()
    
    # Additional backward compatibility methods and properties
    def get(self, key: str, default=None):
        """Dictionary-style get method for backward compatibility."""
        # Map old attribute names to new structure
        mapping = {
            'data_dir': self.data.data_dir,
            'demographics_file': self.data.demographics_file,
            'primary_id_column': self.data.primary_id_column,
            'session_column': self.data.session_column,
            'composite_id_column': self.data.composite_id_column,
            'age_column': self.data.age_column,
            'sex_column': self.data.sex_column,
            'study_site_column': self.data.study_site_column,
            'max_display_rows': self.ui.max_display_rows,
            'cache_ttl_seconds': self.ui.cache_ttl_seconds,
            'backend': self.state.backend,
            'ttl_default': self.state.ttl_default,
            'enable_user_isolation': self.state.enable_user_isolation,
        }
        return mapping.get(key, default)
    
    def get_merge_keys(self):
        """Get merge keys (backward compatibility method)."""
        if self._merge_keys is None:
            # Import here to avoid circular imports
            from data_handling.merge_strategy import MergeKeys
            self._merge_keys = MergeKeys(
                primary_id=self.data.primary_id_column,
                session_id=self.data.session_column,
                composite_id=self.data.composite_id_column
            )
        return self._merge_keys
    
    @property 
    def STATE_BACKEND(self) -> str:
        """Backward compatibility property for state backend."""
        return self.state.backend
    
    @STATE_BACKEND.setter
    def STATE_BACKEND(self, value: str) -> None:
        """Backward compatibility property setter for state backend."""
        self.state.backend = value
    
    @property
    def STATE_ENABLE_USER_ISOLATION(self) -> bool:
        """Backward compatibility property for user isolation."""
        return self.state.enable_user_isolation
    
    @STATE_ENABLE_USER_ISOLATION.setter
    def STATE_ENABLE_USER_ISOLATION(self, value: bool) -> None:
        """Backward compatibility property setter for user isolation."""
        self.state.enable_user_isolation = value
    
    @property
    def STATE_TTL_DEFAULT(self) -> int:
        """Backward compatibility property for state TTL default."""
        return self.state.ttl_default
    
    @STATE_TTL_DEFAULT.setter
    def STATE_TTL_DEFAULT(self, value: int) -> None:
        """Backward compatibility property setter for state TTL default."""
        self.state.ttl_default = value
    
    @property
    def STATE_REDIS_URL(self) -> str:
        """Backward compatibility property for Redis URL."""
        return self.state.redis_url
    
    @STATE_REDIS_URL.setter
    def STATE_REDIS_URL(self, value: str) -> None:
        """Backward compatibility property setter for Redis URL."""
        self.state.redis_url = value
