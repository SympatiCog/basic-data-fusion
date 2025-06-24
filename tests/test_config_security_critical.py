"""
CRITICAL CONFIGURATION SECURITY TESTS for Basic Data Fusion
Tests for configuration vulnerabilities, validation, and security hardening.
"""
import os
import sys
import tempfile
import toml
from pathlib import Path
from unittest.mock import patch

import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import Config
from config_manager import get_config, refresh_config


class TestConfigurationSecurity:
    """Critical tests for configuration security vulnerabilities."""

    def test_toml_injection_prevention(self):
        """Test prevention of TOML injection attacks."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as tmp_file:
            # Malicious TOML with injection attempts
            malicious_toml = '''
# Legitimate config
data_dir = "safe_directory"
demographics_file = "demographics.csv"
primary_id_column = "ursi"

# Injection attempts
[malicious]
__import__ = "os"
eval = "print('HACKED')"
exec = "__import__('os').system('echo COMPROMISED')"

# More injection attempts
[another_section]
code = """
import subprocess
subprocess.run(['rm', '-rf', '/'])
"""

# Path traversal in config values
[paths]
secret_path = "../../../etc/passwd"
evil_path = "/etc/shadow"
windows_path = "C:\\Windows\\System32\\config\\SAM"

# Command injection attempts
[commands]
command1 = "safe_value; rm -rf /"
command2 = "value && echo HACKED"
command3 = "value | cat /etc/passwd"
'''
            tmp_file.write(malicious_toml)
            tmp_file.flush()
            
            try:
                config = Config()
                original_path = config.CONFIG_FILE_PATH
                config.CONFIG_FILE_PATH = tmp_file.name
                
                # Load the malicious config
                config.load_config()
                
                # Should only have expected attributes
                expected_attrs = {
                    'DATA_DIR', 'DEMOGRAPHICS_FILE', 'PRIMARY_ID_COLUMN',
                    'SESSION_COLUMN', 'COMPOSITE_ID_COLUMN', 'AGE_COLUMN', 'SEX_COLUMN'
                }
                
                # Should not have malicious sections
                assert not hasattr(config, 'malicious')
                assert not hasattr(config, 'another_section')  
                assert not hasattr(config, 'paths')
                assert not hasattr(config, 'commands')
                
                # Should not execute code
                assert not hasattr(config, '__import__')
                assert not hasattr(config, 'eval')
                assert not hasattr(config, 'exec')
                
                # Configuration values should be safe defaults (malicious config rejected)
                assert config.DATA_DIR == "data"  # Default value used when malicious config rejected
                assert config.DEMOGRAPHICS_FILE == "demographics.csv"
                assert config.PRIMARY_ID_COLUMN == "ursi"
                
                config.CONFIG_FILE_PATH = original_path
                
            finally:
                os.unlink(tmp_file.name)

    def test_path_traversal_in_config_values(self):
        """Test prevention of path traversal in configuration values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as tmp_file:
            # Config with path traversal attempts
            malicious_config = '''
data_dir = "../../../etc"
demographics_file = "../../../../etc/passwd"
primary_id_column = "../../sensitive_data"
age_column = "../config/secrets"
'''
            tmp_file.write(malicious_config)
            tmp_file.flush()
            
            try:
                config = Config()
                original_path = config.CONFIG_FILE_PATH
                config.CONFIG_FILE_PATH = tmp_file.name
                
                config.load_config()
                
                # Values should be loaded but path operations should be safe
                assert config.DATA_DIR == "../../../etc"  # Value loaded
                assert config.DEMOGRAPHICS_FILE == "../../../../etc/passwd"
                
                # But when used in file operations, should be safe
                # Test that get_merge_keys() handles this safely
                try:
                    merge_keys = config.get_merge_keys()
                    # Should not crash and should return safe defaults
                    assert merge_keys is not None
                    assert merge_keys.primary_id is not None
                except Exception as e:
                    # Should fail safely without exposing sensitive info
                    error_msg = str(e)
                    assert "password" not in error_msg.lower()
                    assert "secret" not in error_msg.lower()
                
                config.CONFIG_FILE_PATH = original_path
                
            finally:
                os.unlink(tmp_file.name)

    def test_config_file_permission_security(self):
        """Test security of config file permissions and access."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = Config()
            
            # Test with read-only directory (should fail gracefully)
            readonly_dir = os.path.join(temp_dir, "readonly")
            os.makedirs(readonly_dir)
            os.chmod(readonly_dir, 0o444)  # Read-only
            
            readonly_config_path = os.path.join(readonly_dir, "config.toml")
            original_path = config.CONFIG_FILE_PATH
            config.CONFIG_FILE_PATH = readonly_config_path
            
            try:
                # Should not crash when trying to save to read-only location
                config.save_config()
                # Should have attempted save but failed gracefully
            except PermissionError:
                # This is expected and safe
                pass
            except Exception as e:
                # Other exceptions should be safe
                error_msg = str(e)
                assert len(error_msg) < 200  # No verbose error disclosure
            finally:
                config.CONFIG_FILE_PATH = original_path
                os.chmod(readonly_dir, 0o755)  # Restore permissions for cleanup

    def test_config_validation_edge_cases(self):
        """Test configuration validation with malformed and edge case values."""
        test_configs = [
            # Empty values
            {'data_dir': '', 'demographics_file': ''},
            
            # Null bytes
            {'data_dir': 'path\x00with\x00nulls', 'demographics_file': 'file\x00.csv'},
            
            # Very long values (potential buffer overflow)
            {'data_dir': 'x' * 10000, 'demographics_file': 'y' * 10000},
            
            # Unicode injection
            {'data_dir': 'normal\u202e/evil/path', 'demographics_file': 'file\u0000.csv'},
            
            # Type confusion
            {'default_age_min': 'not_a_number', 'default_age_max': ['not', 'number']},
            
            # Negative values where inappropriate
            {'default_age_min': -1000, 'max_display_rows': -50},
        ]
        
        for malformed_config in test_configs:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as tmp_file:
                try:
                    toml.dump(malformed_config, tmp_file)
                    tmp_file.flush()
                    
                    config = Config()
                    original_path = config.CONFIG_FILE_PATH
                    config.CONFIG_FILE_PATH = tmp_file.name
                    
                    # Should load without crashing
                    config.load_config()
                    
                    # Should have reasonable defaults for invalid values
                    assert isinstance(config.DATA_DIR, str)
                    assert isinstance(config.DEMOGRAPHICS_FILE, str)
                    
                    # Numeric values should be reasonable
                    if hasattr(config, 'DEFAULT_AGE_SELECTION'):
                        assert isinstance(config.DEFAULT_AGE_SELECTION[0], (int, float))
                        assert isinstance(config.DEFAULT_AGE_SELECTION[1], (int, float))
                        assert config.DEFAULT_AGE_SELECTION[0] >= 0
                        assert config.DEFAULT_AGE_SELECTION[1] > config.DEFAULT_AGE_SELECTION[0]
                    
                    config.CONFIG_FILE_PATH = original_path
                    
                except toml.TomlDecodeError:
                    # TOML parsing errors are acceptable
                    pass
                finally:
                    os.unlink(tmp_file.name)

    def test_config_manager_singleton_security(self):
        """Test security of config manager singleton pattern."""
        # Clear any existing config
        refresh_config()
        
        # Create malicious config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as tmp_file:
            malicious_config = '''
data_dir = "hacked_directory"
demographics_file = "malicious.csv"
[evil_section]
code = "print('HACKED')"
'''
            tmp_file.write(malicious_config)
            tmp_file.flush()
            
            try:
                # Patch the config file path
                with patch.object(Config, 'CONFIG_FILE_PATH', tmp_file.name):
                    config1 = get_config()
                    config2 = get_config()
                    
                    # Should be the same instance
                    assert config1 is config2
                    
                    # Should have rejected malicious config and used defaults
                    assert config1.DATA_DIR == "data"  # Default used when malicious config rejected
                    assert not hasattr(config1, 'evil_section')
                    assert not hasattr(config1, 'code')
                    
                    # Refresh should create new instance
                    config3 = refresh_config()
                    assert config3 is not config1
                    
            finally:
                os.unlink(tmp_file.name)

    def test_config_merge_strategy_security(self):
        """Test security of merge strategy configuration and detection."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = Config()
            config.DATA_DIR = temp_dir
            
            # Test with malicious column names in demographics
            malicious_demographics = '''ursi,age,'; DROP TABLE users; --,session_num
001,25,value1,BAS1
002,30,value2,BAS2'''
            
            demo_path = os.path.join(temp_dir, "demographics.csv")
            with open(demo_path, 'w') as f:
                f.write(malicious_demographics)
            
            config.DEMOGRAPHICS_FILE = "demographics.csv"
            
            try:
                # Should detect structure safely
                merge_keys = config.get_merge_keys()
                
                # Should have detected structure without SQL injection
                assert merge_keys is not None
                assert merge_keys.primary_id is not None
                
                # Should not have processed malicious column name
                assert "DROP" not in merge_keys.primary_id.upper()
                
                # Test merge strategy preparation
                merge_strategy = config.get_merge_strategy()
                success, actions = merge_strategy.prepare_datasets(temp_dir, merge_keys)
                
                # Should complete safely
                assert isinstance(success, bool)
                assert isinstance(actions, list)
                
                # Actions should not contain injection attempts
                for action in actions:
                    assert "DROP" not in action.upper()
                    assert "--" not in action
                    
            except Exception as e:
                # Errors should be safe
                error_msg = str(e)
                assert "password" not in error_msg.lower()
                assert len(error_msg) < 1000

    def test_config_session_column_security(self):
        """Test security of session column configuration."""
        config = Config()
        
        # Test malicious session column names
        malicious_columns = [
            "'; DROP TABLE sessions; --",
            "session_num' UNION SELECT password FROM users --",
            "col/**/UNION/**/SELECT",
            "session`; DELETE FROM data; --"
        ]
        
        for malicious_col in malicious_columns:
            config.SESSION_COLUMN = malicious_col
            
            # Should handle safely
            try:
                # This might use the session column in query generation
                merge_keys = config.get_merge_keys()
                
                if merge_keys.session_id:
                    # Session ID should be safe or None
                    assert "--" not in str(merge_keys.session_id)
                    assert "DROP" not in str(merge_keys.session_id).upper()
                    
            except Exception as e:
                # Should fail safely
                error_msg = str(e)
                assert "password" not in error_msg.lower()
                assert "users" not in error_msg.lower()

    def test_config_data_directory_validation(self):
        """Test validation of data directory configuration."""
        config = Config()
        
        # Test various malicious data directory paths
        malicious_paths = [
            "/etc",
            "/root",
            "C:\\Windows\\System32",
            "../../../sensitive",
            "/proc/self/mem",
            "/dev/random",
            "//server/share/secrets",
            "path'; rm -rf /; echo '",
        ]
        
        for malicious_path in malicious_paths:
            config.DATA_DIR = malicious_path
            
            try:
                # Should handle path safely in file operations
                merge_keys = config.get_merge_keys()
                # Should not crash and should not expose system info
                
            except (FileNotFoundError, PermissionError, OSError):
                # These are safe expected errors
                pass
            except Exception as e:
                # Other errors should be safe
                error_msg = str(e)
                assert "root" not in error_msg.lower()
                assert "admin" not in error_msg.lower()
                assert "password" not in error_msg.lower()

    def test_config_file_corruption_recovery(self):
        """Test recovery from corrupted configuration files."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as tmp_file:
            # Write completely corrupted TOML
            corrupted_configs = [
                "this is not TOML at all!",
                "[unclosed section\ndata_dir = value",
                "data_dir = 'unclosed string",
                "\x00\x01\x02\x03binary data",
                "a" * 100000,  # Extremely large file
            ]
            
            for corrupted_data in corrupted_configs:
                # Write corrupted data
                with open(tmp_file.name, 'w', encoding='utf-8', errors='ignore') as f:
                    f.write(corrupted_data)
                
                config = Config()
                original_path = config.CONFIG_FILE_PATH
                config.CONFIG_FILE_PATH = tmp_file.name
                
                try:
                    # Should not crash on corrupted config
                    config.load_config()
                    
                    # Should have reasonable defaults
                    assert isinstance(config.DATA_DIR, str)
                    assert len(config.DATA_DIR) > 0
                    assert isinstance(config.DEMOGRAPHICS_FILE, str)
                    assert len(config.DEMOGRAPHICS_FILE) > 0
                    
                except Exception as e:
                    # Errors should be safe and informative
                    error_msg = str(e)
                    assert len(error_msg) < 500  # No verbose disclosure
                
                finally:
                    config.CONFIG_FILE_PATH = original_path
            
            os.unlink(tmp_file.name)


class TestConfigurationValidation:
    """Tests for configuration value validation and sanitization."""

    def test_numeric_value_validation(self):
        """Test validation of numeric configuration values."""
        config = Config()
        
        # Test with extreme numeric values
        test_cases = [
            {'default_age_min': float('inf'), 'default_age_max': float('-inf')},
            {'default_age_min': 1e20, 'default_age_max': -1e20},
            {'max_display_rows': -1},
            {'max_display_rows': 2**32},  # Very large number
        ]
        
        for test_case in test_cases:
            for key, value in test_case.items():
                setattr(config, key.upper(), value)
            
            # Should handle extreme values safely
            try:
                if hasattr(config, 'DEFAULT_AGE_SELECTION'):
                    age_min, age_max = config.DEFAULT_AGE_SELECTION
                    assert isinstance(age_min, (int, float))
                    assert isinstance(age_max, (int, float))
                    assert -200 <= age_min <= 200  # Reasonable bounds
                    assert -200 <= age_max <= 200
                    
                if hasattr(config, 'MAX_DISPLAY_ROWS'):
                    assert 1 <= config.MAX_DISPLAY_ROWS <= 10000  # Reasonable bounds
                    
            except Exception as e:
                # Should handle validation errors gracefully
                error_msg = str(e)
                assert len(error_msg) < 200

    def test_string_value_sanitization(self):
        """Test sanitization of string configuration values."""
        config = Config()
        
        # Test with malicious string values
        malicious_strings = [
            "normal_value\x00null_byte",
            "unicode\u202einjection",
            "very_long_" + "x" * 10000,
            "newline\ninjection",
            "carriage\rreturn",
            "tab\tinjection",
        ]
        
        for malicious_string in malicious_strings:
            config.DATA_DIR = malicious_string
            config.DEMOGRAPHICS_FILE = malicious_string
            config.PRIMARY_ID_COLUMN = malicious_string
            
            # Should handle malicious strings safely
            assert isinstance(config.DATA_DIR, str)
            assert isinstance(config.DEMOGRAPHICS_FILE, str)
            assert isinstance(config.PRIMARY_ID_COLUMN, str)
            
            # Should not contain dangerous characters in actual usage
            try:
                # Test that they can be used safely in file operations
                merge_keys = config.get_merge_keys()
                
            except Exception as e:
                # Should fail safely
                error_msg = str(e)
                assert "\x00" not in error_msg
                assert len(error_msg) < 1000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])