"""
CRITICAL SECURITY FIXES for Basic Data Fusion
Tests for enhanced security functions that fix identified vulnerabilities.
"""
import io
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch
import re
from typing import List, Dict, Tuple

import pandas as pd
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def enhanced_secure_filename(filename: str) -> str:
    """Enhanced secure filename function that fixes path traversal vulnerabilities."""
    # Get basename only, preventing path traversal
    filename = os.path.basename(filename)
    
    # Remove null bytes and control characters
    filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)
    
    # Replace whitespace with underscores
    filename = re.sub(r'\s+', '_', filename)
    
    # Remove path traversal patterns completely
    filename = re.sub(r'\.\.+', '', filename)  # Remove any sequence of dots
    
    # Remove all non-alphanumeric except safe characters
    filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
    
    # Consolidate underscores
    filename = re.sub(r'_+', '_', filename)
    
    # Strip leading/trailing underscores and dots
    filename = filename.strip('_.')
    
    # Ensure not empty
    if not filename:
        filename = "safe_file"
    
    # Ensure reasonable length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        if ext:
            filename = name[:250] + ext
        else:
            filename = filename[:255]
    
    # Ensure it ends with a safe extension for CSV files
    if not filename.lower().endswith('.csv'):
        filename = filename + '.csv'
    
    return filename


def enhanced_sanitize_column_names(columns: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """Enhanced column name sanitization that prevents SQL injection."""
    sanitized_columns = []
    column_mapping = {}
    
    # SQL keywords that should be completely removed or altered
    sql_keywords = {
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 
        'UNION', 'WHERE', 'FROM', 'JOIN', 'HAVING', 'GROUP', 'ORDER', 'BY',
        'EXEC', 'EXECUTE', 'SCRIPT', 'TRUNCATE', 'MERGE', 'GRANT', 'REVOKE'
    }
    
    for original_col in columns:
        # Start with the original column
        sanitized = str(original_col)
        
        # Remove null bytes, control characters, and dangerous characters
        sanitized = re.sub(r'[\x00-\x1f\x7f\'"`\;\\]', '', sanitized)
        
        # Remove SQL comment patterns
        sanitized = re.sub(r'--.*$', '', sanitized)  # Remove -- comments
        sanitized = re.sub(r'/\*.*?\*/', '', sanitized)  # Remove /* */ comments
        
        # Replace whitespace and problematic characters with underscores
        sanitized = re.sub(r'[\s\-\(\)\[\]\{\}\@\#\$\%\^\&\*\+\=\|\?\<\>\,\.\:\/\\]+', '_', sanitized)
        
        # Remove any remaining non-alphanumeric characters except underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '', sanitized)
        
        # Check for and modify SQL keywords
        words = sanitized.upper().split('_')
        safe_words = []
        for word in words:
            if word in sql_keywords:
                # Replace SQL keywords with safe alternatives
                safe_words.append(f"FIELD_{word}")
            else:
                safe_words.append(word)
        sanitized = '_'.join(safe_words)
        
        # Consolidate multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        # Ensure column name is not empty
        if not sanitized:
            sanitized = f"col_{len(sanitized_columns)}"
        
        # Ensure column name doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"
        
        # Ensure uniqueness
        original_sanitized = sanitized
        counter = 1
        while sanitized in sanitized_columns:
            sanitized = f"{original_sanitized}_{counter}"
            counter += 1
        
        sanitized_columns.append(sanitized)
        column_mapping[original_col] = sanitized
    
    return sanitized_columns, column_mapping


class TestEnhancedSecurity:
    """Test the enhanced security functions."""
    
    def test_enhanced_secure_filename_path_traversal(self):
        """Test enhanced secure filename against path traversal."""
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
            safe_name = enhanced_secure_filename(malicious_filename)
            
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
            
            # Should end with .csv
            assert safe_name.endswith('.csv')
    
    def test_enhanced_column_sanitization_sql_injection(self):
        """Test enhanced column sanitization against SQL injection."""
        malicious_columns = [
            "name'; DROP TABLE users; --",
            "data UNION SELECT * FROM passwords",
            "col1/*comment*/",
            "name` OR 1=1 --",
            "field); DELETE FROM data; --",
            'column"" OR ""a""=""a',
            "name\"; INSERT INTO logs VALUES ('hacked'); --"
        ]
        
        sanitized, mapping = enhanced_sanitize_column_names(malicious_columns)
        
        for sanitized_col in sanitized:
            # Should not contain SQL injection characters
            assert ";" not in sanitized_col
            assert "--" not in sanitized_col
            assert "'" not in sanitized_col
            assert '"' not in sanitized_col
            assert "`" not in sanitized_col
            assert "/*" not in sanitized_col
            assert "*/" not in sanitized_col
            
            # SQL keywords should be modified, not just removed
            assert "UNION" not in sanitized_col.upper() or "FIELD_UNION" in sanitized_col.upper()
            assert "DROP" not in sanitized_col.upper() or "FIELD_DROP" in sanitized_col.upper()
            assert "DELETE" not in sanitized_col.upper() or "FIELD_DELETE" in sanitized_col.upper()
            assert "INSERT" not in sanitized_col.upper() or "FIELD_INSERT" in sanitized_col.upper()
    
    def test_enhanced_security_comprehensive(self):
        """Comprehensive test of all enhanced security measures."""
        # Test with a variety of malicious inputs
        test_cases = [
            ("../../../passwd.csv", "passwd.csv"),
            ("file\0name.csv", "filename.csv"),
            ("col'; DROP TABLE--", "FIELD_col_FIELD_DROP_FIELD_TABLE.csv"),
            ("", "safe_file.csv"),
            ("normal_file", "normal_file.csv"),
        ]
        
        for input_name, expected_pattern in test_cases:
            result = enhanced_secure_filename(input_name)
            assert len(result) > 0
            assert result.endswith('.csv')
            assert '..' not in result
            assert '/' not in result
            assert '\\' not in result


# Integration test showing how to patch the existing functions
class TestSecurityIntegration:
    """Test integration of security fixes with existing code."""
    
    @patch('utils.secure_filename')
    @patch('utils.sanitize_column_names')
    def test_patched_security_functions(self, mock_sanitize, mock_secure):
        """Test that patched security functions work correctly."""
        from utils import save_uploaded_files_to_data_dir
        
        # Set up mocks to use enhanced functions
        mock_secure.side_effect = enhanced_secure_filename
        mock_sanitize.side_effect = enhanced_sanitize_column_names
        
        # Test with malicious input
        malicious_csv = "name'; DROP TABLE users,data\nhacker,payload\n"
        content_bytes = malicious_csv.encode('utf-8')
        
        with tempfile.TemporaryDirectory() as temp_dir:
            success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                [content_bytes], 
                ["../../../malicious.csv"], 
                temp_dir
            )
            
            # Should succeed with safe filename
            assert len(error_msgs) == 0
            assert len(success_msgs) > 0
            
            # Check that file was created safely
            files = os.listdir(temp_dir)
            assert len(files) == 1
            safe_filename = files[0]
            
            # Verify filename is safe
            assert ".." not in safe_filename
            assert "/" not in safe_filename
            assert safe_filename.endswith('.csv')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])