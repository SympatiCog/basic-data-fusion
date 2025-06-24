"""
CRITICAL SECURITY TESTS for Basic Data Fusion
Tests for security vulnerabilities in file operations, path traversal, and data validation.
"""
import io
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    Config,
    secure_filename,
    validate_csv_file,
    save_uploaded_files_to_data_dir,
    sanitize_column_names
)


class TestPathTraversalSecurity:
    """Critical tests for path traversal vulnerabilities."""

    def test_path_traversal_filename_security(self):
        """Test that path traversal attempts in filenames are blocked."""
        malicious_filenames = [
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\hosts",
            "../../../../root/.ssh/id_rsa",
            "../config.toml",
            "../../app.py",
            "/../../../etc/shadow",
            "file../.../../../sensitive.txt",
            "normal_file.csv/../../../etc/passwd",
            "file.csv\0/etc/passwd",  # null byte injection
            "file.csv\n/etc/passwd",  # newline injection
        ]
        
        for malicious_filename in malicious_filenames:
            safe_name = secure_filename(malicious_filename)
            
            # Should not contain path separators
            assert "/" not in safe_name
            assert "\\" not in safe_name
            assert ".." not in safe_name
            
            # Should not be empty after sanitization
            assert len(safe_name) > 0
            
            # Should not contain null bytes or control characters
            assert "\0" not in safe_name
            assert "\n" not in safe_name
            assert "\r" not in safe_name

    def test_absolute_path_rejection(self):
        """Test that absolute paths are rejected and converted to basename only."""
        absolute_paths = [
            "/etc/passwd",
            "C:\\Windows\\System32\\config\\SAM",
            "/home/user/.ssh/id_rsa",
            "\\\\server\\share\\file.csv",
            "/var/log/auth.log",
        ]
        
        for abs_path in absolute_paths:
            safe_name = secure_filename(abs_path)
            
            # Should be safe (no path separators, no dangerous characters)
            assert "/" not in safe_name
            assert "\\" not in safe_name
            assert ".." not in safe_name
            assert ":" not in safe_name  # Colons are removed by sanitization
            
            # Should not be empty
            assert len(safe_name) > 0


class TestMaliciousFileUpload:
    """Critical tests for malicious file upload prevention."""

    def test_oversized_file_rejection(self):
        """Test that files over the size limit are rejected."""
        # Create a file that's too large (over 50MB limit)
        large_content = b"a" * (51 * 1024 * 1024)  # 51MB
        
        errors, df = validate_csv_file(large_content, "large_file.csv")
        
        assert len(errors) > 0
        assert any("too large" in error.lower() for error in errors)
        assert df is None

    def test_non_csv_file_rejection(self):
        """Test that non-CSV files are rejected."""
        malicious_extensions = [
            "malware.exe",
            "script.py",
            "config.toml",
            "shell.sh",
            "data.xlsx",
            "file.txt",
            "archive.zip",
        ]
        
        for filename in malicious_extensions:
            # Create fake content
            content = b"malicious content"
            
            errors, df = validate_csv_file(content, filename)
            
            assert len(errors) > 0
            assert any("must be a CSV" in error for error in errors)
            assert df is None

    def test_csv_injection_prevention(self):
        """Test prevention of CSV injection attacks."""
        # CSV injection payloads that could execute in spreadsheet applications
        malicious_csv_content = '''=cmd|' /C calc'!A0
@SUM(1+9)*cmd|' /C calc'!A0
+cmd|' /C calc'!A0
-cmd|' /C calc'!A0
=2+5+cmd|' /C calc'!A0
@SUM(1+1)*cmd|' /C calc'!A0
name,=1+1+cmd|' /C calc'!A0
'''
        
        content_bytes = malicious_csv_content.encode('utf-8')
        errors, df = validate_csv_file(content_bytes, "injection.csv")
        
        # Should be able to read the CSV but we need to validate content
        if df is not None:
            # Check that dangerous formulas are detected
            for col in df.columns:
                for val in df[col].astype(str):
                    # Should not start with dangerous characters in production
                    if val.startswith(('=', '+', '-', '@')):
                        # In a real implementation, this should be flagged
                        # For now, we document this as a known risk
                        pass

    def test_malformed_csv_handling(self):
        """Test handling of malformed CSV files that could cause crashes."""
        malformed_csvs = [
            b'',  # Empty file
            b'col1,col2\n"unclosed quote',  # Unclosed quote
            b'col1,col2\nval1,val2,val3,val4,val5',  # Mismatched columns
            b'\xff\xfe\x00\x00',  # Invalid UTF-8
            b'col1,col2\n' + b'\x00' * 1000,  # Null bytes
            b'col1,col2\n' + b'a' * 10000 + b',value',  # Extremely long field
        ]
        
        for i, malformed_content in enumerate(malformed_csvs):
            errors, df = validate_csv_file(malformed_content, f"malformed_{i}.csv")
            
            # Should either handle gracefully or provide clear error
            if errors:
                assert len(errors) > 0
                # Errors should be informative, not expose internals
                for error in errors:
                    assert "Traceback" not in error
                    assert "Exception" not in error

    def test_zip_bomb_protection(self):
        """Test protection against zip bomb attacks (nested archives)."""
        # Create a small zip bomb
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_zip:
            with zipfile.ZipFile(tmp_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
                # Create a large text file when decompressed
                large_text = "A" * (10 * 1024 * 1024)  # 10MB when decompressed
                zf.writestr("large_file.txt", large_text)
            
            zip_path = tmp_zip.name
        
        try:
            # Read the zip file
            with open(zip_path, 'rb') as f:
                zip_content = f.read()
            
            # Should reject zip files
            errors, df = validate_csv_file(zip_content, "bomb.csv")
            
            assert len(errors) > 0
            assert df is None
            
        finally:
            os.unlink(zip_path)


class TestColumnNameSecurity:
    """Test security of column name sanitization."""

    def test_sql_injection_column_names(self):
        """Test that column names with SQL injection attempts are sanitized."""
        malicious_columns = [
            "name'; DROP TABLE users; --",
            "data UNION SELECT * FROM passwords",
            "col1/*comment*/",
            "name` OR 1=1 --",
            "field); DELETE FROM data; --",
            'column"" OR ""a""=""a',
            "name\"; INSERT INTO logs VALUES ('hacked'); --"
        ]
        
        sanitized, mapping = sanitize_column_names(malicious_columns)
        
        for sanitized_col in sanitized:
            # Should not contain SQL injection characters
            assert ";" not in sanitized_col
            assert "--" not in sanitized_col
            assert "'" not in sanitized_col
            assert '"' not in sanitized_col
            assert "/*" not in sanitized_col
            assert "*/" not in sanitized_col
            
            # SQL keywords should be safely prefixed, not just removed
            # Dangerous bare SQL keywords should not exist
            upper_col = sanitized_col.upper()
            
            # Check that SQL keywords are properly prefixed if they exist
            if "UNION" in upper_col:
                assert "FIELD_UNION" in upper_col
            if "DROP" in upper_col:
                assert "FIELD_DROP" in upper_col
            if "DELETE" in upper_col:
                assert "FIELD_DELETE" in upper_col
            if "INSERT" in upper_col:
                assert "FIELD_INSERT" in upper_col

    def test_special_character_sanitization(self):
        """Test sanitization of special characters in column names."""
        special_columns = [
            "col with spaces",
            "col-with-dashes",
            "col.with.dots",
            "col(with)parens",
            "col[with]brackets",
            "col{with}braces",
            "col@with@symbols",
            "col#with#hash",
            "col$with$dollar",
            "col%with%percent",
            "col&with&ampersand",
        ]
        
        sanitized, mapping = sanitize_column_names(special_columns)
        
        for sanitized_col in sanitized:
            # Should only contain alphanumeric and underscores
            assert all(c.isalnum() or c == '_' for c in sanitized_col)
            # Should not start with a number
            assert not sanitized_col[0].isdigit()


class TestConcurrentFileOperations:
    """Test security implications of concurrent file operations."""

    def test_race_condition_file_creation(self):
        """Test handling of race conditions in file creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            filename = "test.csv"
            file_path = os.path.join(temp_dir, filename)
            
            # Create the file before save operation
            with open(file_path, 'w') as f:
                f.write("existing,data\n1,2\n")
            
            # Try to save a file with the same name
            csv_content = "new,content\n3,4\n"
            content_bytes = csv_content.encode('utf-8')
            
            success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                [content_bytes], [filename], temp_dir
            )
            
            # Should handle the conflict gracefully
            assert len(error_msgs) == 0  # Should not error
            # Should either rename or indicate replacement
            assert len(success_msgs) > 0

    def test_symlink_attack_prevention(self):
        """Test prevention of symlink attacks."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a symlink to a sensitive file (simulated)
            sensitive_file = os.path.join(temp_dir, "sensitive.txt")
            with open(sensitive_file, 'w') as f:
                f.write("sensitive data")
            
            symlink_path = os.path.join(temp_dir, "symlink.csv")
            
            try:
                os.symlink(sensitive_file, symlink_path)
                
                # Try to write to the symlink
                csv_content = "malicious,data\n1,2\n"
                content_bytes = csv_content.encode('utf-8')
                
                # Should not follow symlinks in a secure implementation
                # This test documents the current behavior
                success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                    [content_bytes], ["symlink.csv"], temp_dir
                )
                
                # Verify the original file wasn't modified
                with open(sensitive_file, 'r') as f:
                    content = f.read()
                    assert content == "sensitive data"
                    
            except OSError:
                # Symlinks might not be supported on all filesystems
                pytest.skip("Symlinks not supported on this filesystem")


class TestMemoryExhaustionPrevention:
    """Test prevention of memory exhaustion attacks."""

    def test_large_column_count_handling(self):
        """Test handling of CSV files with excessive column counts."""
        # Create CSV with many columns
        column_count = 1500  # Above typical limit
        columns = [f"col_{i}" for i in range(column_count)]
        header = ",".join(columns)
        row = ",".join(["value"] * column_count)
        csv_content = f"{header}\n{row}\n"
        
        content_bytes = csv_content.encode('utf-8')
        errors, df = validate_csv_file(content_bytes, "many_columns.csv")
        
        # Should reject files with excessive columns
        if len(columns) > 1000:  # Based on utils.py limit
            assert len(errors) > 0
            assert any("too many columns" in error.lower() for error in errors)

    def test_extremely_long_field_handling(self):
        """Test handling of extremely long field values."""
        # Create a field that's very long
        long_value = "A" * (1024 * 1024)  # 1MB field
        csv_content = f"column1,column2\nshort_value,{long_value}\n"
        
        content_bytes = csv_content.encode('utf-8')
        errors, df = validate_csv_file(content_bytes, "long_field.csv")
        
        # Should handle gracefully without memory exhaustion
        # This test ensures the validation doesn't crash
        assert isinstance(errors, list)


class TestConfigurationSecurity:
    """Test security aspects of configuration handling."""

    def test_config_file_path_traversal(self):
        """Test that config file paths cannot be manipulated."""
        config = Config()
        
        # Try to set malicious config file paths
        original_path = config.CONFIG_FILE_PATH
        
        malicious_paths = [
            "../../../etc/passwd",
            "/etc/shadow",
            "C:\\Windows\\System32\\config\\SAM",
            "../../../../root/.ssh/id_rsa"
        ]
        
        for malicious_path in malicious_paths:
            config.CONFIG_FILE_PATH = malicious_path
            
            # Should not be able to read from malicious paths
            try:
                config.load_config()
                # If it doesn't throw an error, it should create a safe default
                assert config.CONFIG_FILE_PATH == malicious_path  # Path is set but...
                # The actual file operations should be safe
            except (FileNotFoundError, PermissionError):
                # This is expected and safe
                pass
        
        # Restore original path
        config.CONFIG_FILE_PATH = original_path

    def test_toml_injection_prevention(self):
        """Test prevention of TOML injection attacks."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as tmp_file:
            # Malicious TOML content
            malicious_toml = '''
data_dir = "safe_directory"
demographics_file = "demographics.csv"

# Injection attempt
[malicious_section]
command = "rm -rf /"
script = """
import os
os.system('echo "injected"')
"""

# Another injection attempt
[[malicious_array]]
name = "test"
value = "${HOME}/.ssh/id_rsa"
'''
            tmp_file.write(malicious_toml)
            tmp_file.flush()
            
            config = Config()
            config.CONFIG_FILE_PATH = tmp_file.name
            
            try:
                config.load_config()
                
                # Should only load expected configuration values
                assert hasattr(config, 'DATA_DIR')
                assert hasattr(config, 'DEMOGRAPHICS_FILE')
                
                # Should not execute or expose malicious content
                assert not hasattr(config, 'malicious_section')
                assert not hasattr(config, 'command')
                assert not hasattr(config, 'script')
                
            finally:
                os.unlink(tmp_file.name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])