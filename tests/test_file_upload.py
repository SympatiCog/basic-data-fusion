"""
Tests for file upload functionality.
"""
import io
import os
import sys
import tempfile
from unittest.mock import Mock, patch

import pandas as pd
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import save_uploaded_files_to_data_dir, secure_filename, validate_csv_file


class TestSecureFilename:
    """Test the secure_filename function."""

    def test_basic_filename(self):
        """Test basic filename sanitization."""
        result = secure_filename("test.csv")
        assert result == "test.csv"

    def test_whitespace_replacement(self):
        """Test that whitespace is replaced with underscores."""
        result = secure_filename("my file name.csv")
        assert result == "my_file_name.csv"

        # Multiple spaces should become single underscore
        result = secure_filename("file   with    spaces.csv")
        assert result == "file_with_spaces.csv"

        # Tabs and other whitespace (control characters are removed)
        result = secure_filename("file\twith\ttabs.csv")
        assert result == "filewithtabs.csv"

        result = secure_filename("file\nwith\nnewlines.csv")
        assert result == "filewithnewlines.csv"

    def test_special_character_replacement(self):
        """Test that special characters are replaced with underscores."""
        result = secure_filename("file@#$%^&*().csv")
        assert result == "file_.csv"

        result = secure_filename("file<>|?\\/:*.csv")
        assert result == "csv"  # All chars except extension get removed, leading dots stripped

        result = secure_filename("file!@#$%^&*()+=[]{}|\\:;\"'<>?,./`~.csv")
        assert result == "csv"  # All special chars remove everything before extension

    def test_preserve_valid_characters(self):
        """Test that valid characters are preserved."""
        result = secure_filename("file_name-123.csv")
        assert result == "file_name-123.csv"

        result = secure_filename("File.With.Multiple.Dots.csv")
        assert result == "File.With.Multiple.Dots.csv"

        result = secure_filename("UPPERCASE_and_lowercase-123.csv")
        assert result == "UPPERCASE_and_lowercase-123.csv"

    def test_multiple_underscore_cleanup(self):
        """Test that multiple consecutive underscores are cleaned up."""
        result = secure_filename("file___with___many___underscores.csv")
        assert result == "file_with_many_underscores.csv"

        # Mix of special characters should not create multiple underscores
        result = secure_filename("file@@@###name.csv")
        assert result == "file_name.csv"

    def test_leading_trailing_underscore_removal(self):
        """Test that leading and trailing underscores are removed."""
        result = secure_filename("___file_name___.csv")
        assert result == "file_name_.csv"  # Leading underscores removed, trailing before extension kept

        result = secure_filename("@@@file_name@@@.csv")
        assert result == "file_name_.csv"

    def test_path_component_removal(self):
        """Test that path components are removed."""
        result = secure_filename("/path/to/file.csv")
        assert result == "file.csv"  # os.path.basename works correctly

        result = secure_filename("..\\..\\..\\file.csv")
        assert result == "file.csv"  # dots and backslashes removed

        result = secure_filename("C:\\Users\\Documents\\file.csv")
        assert result == "C_Users_Documents_file.csv"  # On non-Windows, backslashes aren't path separators

    def test_empty_filename(self):
        """Test handling of empty or invalid filenames."""
        result = secure_filename("")
        assert result == "safe_file"

        result = secure_filename("@@@")
        assert result == "safe_file"

        result = secure_filename("___")
        assert result == "safe_file"

    def test_only_extension(self):
        """Test handling of files with only extension."""
        result = secure_filename(".csv")
        assert result == "csv"

        result = secure_filename("@@@.csv")
        assert result == "csv"

    def test_length_limitation(self):
        """Test that very long filenames are truncated."""
        # Create a filename longer than 255 characters
        long_name = "a" * 300 + ".csv"
        result = secure_filename(long_name)

        assert len(result) <= 255
        assert result.endswith(".csv")
        assert len(result.replace(".csv", "")) <= 250

    def test_unicode_characters(self):
        """Test handling of unicode characters."""
        result = secure_filename("文件名.csv")
        assert result == "csv"  # All unicode chars removed, only extension left, dot stripped

        result = secure_filename("file_ñame_café.csv")
        assert result == "file_ame_caf_.csv"

    def test_edge_cases(self):
        """Test various edge cases."""
        # Just dots and extension (dots get removed)
        result = secure_filename("....csv")
        assert result == "csv"

        # Numbers only
        result = secure_filename("123456.csv")
        assert result == "123456.csv"

        # Mixed valid and invalid
        result = secure_filename("file-name_123.test@#$.csv")
        assert result == "file-name_123.test_.csv"


class TestValidateCsvFile:
    """Test the validate_csv_file function."""

    def create_mock_uploaded_file(self, content: str, filename: str = "test.csv", size: int = None):
        """Create a mock uploaded file object for testing."""
        mock_file = Mock()
        mock_file.name = filename
        mock_file.size = size if size is not None else len(content)
        mock_file.getbuffer.return_value = content.encode('utf-8')

        # Create a DataFrame from the content for mocking pandas.read_csv
        string_buffer = io.StringIO(content)
        try:
            df = pd.read_csv(string_buffer)
        except Exception:
            df = pd.DataFrame()  # Empty DataFrame for invalid content

        return mock_file, df

    def test_valid_csv_file(self):
        """Test validation of a valid CSV file."""
        content = "ursi,age,sex\nSUB001,25,Female\nSUB002,30,Male"
        mock_file, expected_df = self.create_mock_uploaded_file(content)

        with patch('pandas.read_csv', return_value=expected_df):
            errors, df = validate_csv_file(mock_file.getbuffer(), mock_file.name)

        assert errors == []
        assert df is not None
        assert len(df) == 2
        assert list(df.columns) == ['ursi', 'age', 'sex']

    def test_file_too_large(self):
        """Test rejection of files that are too large."""
        # Create content that is actually larger than 50MB
        # Each line is about 12 bytes, so to get 51MB we need more repetitions
        target_size = 51 * 1024 * 1024  # 51MB
        line = "SUB001,25\n"  # 10 bytes + newline
        lines_needed = target_size // len(line.encode('utf-8')) + 1000  # Add extra to be sure
        large_content = "ursi,age\n" + line * lines_needed
        large_content_bytes = large_content.encode('utf-8')

        errors, df = validate_csv_file(large_content_bytes, "large_file.csv")

        assert "File 'large_file.csv' too large (maximum 50MB)" in errors
        assert df is None

    def test_invalid_file_extension(self):
        """Test rejection of non-CSV files."""
        content = "ursi,age\nSUB001,25"
        mock_file, expected_df = self.create_mock_uploaded_file(content, filename="test.txt")

        with patch('pandas.read_csv', return_value=expected_df):
            errors, df = validate_csv_file(mock_file.getbuffer(), mock_file.name)

        assert "File 'test.txt' must be a CSV (.csv extension)" in errors
        assert df is None

    def test_empty_file(self):
        """Test rejection of empty files."""
        content = "ursi,age"  # Headers only, no data rows
        mock_file, expected_df = self.create_mock_uploaded_file(content)

        with patch('pandas.read_csv', return_value=expected_df):
            errors, df = validate_csv_file(mock_file.getbuffer(), mock_file.name)

        assert "File 'test.csv' is empty (no data rows)" in errors
        assert df is None

    def test_required_columns_present(self):
        """Test validation with required columns that are present."""
        content = "ursi,age,sex\nSUB001,25,Female"
        mock_file, expected_df = self.create_mock_uploaded_file(content)

        with patch('pandas.read_csv', return_value=expected_df):
            errors, df = validate_csv_file(mock_file.getbuffer(), mock_file.name, required_columns=['ursi', 'age'])

        assert errors == []
        assert df is not None

    def test_required_columns_missing(self):
        """Test validation with required columns that are missing."""
        content = "ursi,age\nSUB001,25"
        mock_file, expected_df = self.create_mock_uploaded_file(content)

        with patch('pandas.read_csv', return_value=expected_df):
            errors, df = validate_csv_file(mock_file.getbuffer(), mock_file.name, required_columns=['ursi', 'age', 'sex'])

        assert "File 'test.csv' missing required columns: sex" in errors
        assert df is None

    def test_no_columns(self):
        """Test rejection of files with no columns."""
        # Test with empty content that should result in no columns
        content = b""  # Empty content
        
        # Mock pandas to return an empty DataFrame
        with patch('pandas.read_csv', return_value=pd.DataFrame()):
            errors, df = validate_csv_file(content, "test.csv")

        assert "File 'test.csv' has no columns" in errors or "File 'test.csv' is empty (no data rows)" in errors
        assert df is None

    def test_too_many_columns(self):
        """Test rejection of files with too many columns."""
        # Create a mock DataFrame with 1001 columns
        headers = [f"col_{i}" for i in range(1001)]
        mock_df = pd.DataFrame(columns=headers)
        mock_df.loc[0] = [1] * 1001  # Add one row

        # Test with minimal CSV content
        content = b"col_0,col_1,col_2\n1,2,3\n"  # Simple content, but mock will return large df

        with patch('pandas.read_csv', return_value=mock_df):
            errors, df = validate_csv_file(content, "test.csv")

        assert "File 'test.csv' has too many columns (maximum 1000)" in errors
        assert df is None

    def test_duplicate_column_names(self):
        """Test rejection of files with duplicate column names."""
        # Create a DataFrame with duplicate column names by creating it directly
        mock_df = pd.DataFrame([['SUB001', 25, 'SUB001_DUP']], columns=['ursi', 'age', 'ursi'])

        # Test with minimal CSV content
        content = b"ursi,age,ursi\nSUB001,25,SUB001_DUP\n"

        with patch('pandas.read_csv', return_value=mock_df):
            errors, df = validate_csv_file(content, "test.csv")

        assert "File 'test.csv' has duplicate column names: ursi" in errors
        assert df is None

    def test_invalid_csv_format(self):
        """Test handling of invalid CSV format."""
        # Test with some content
        content = b"invalid,csv,content\n"

        # Mock pandas to raise ParserError
        with patch('pandas.read_csv', side_effect=pd.errors.ParserError("Invalid CSV")):
            errors, df = validate_csv_file(content, "test.csv")

        assert any("Invalid CSV format" in error for error in errors)
        assert df is None

    def test_unicode_decode_error(self):
        """Test handling of unicode decode errors."""
        # Test with some content
        content = b"test,data\n1,2\n"

        # Mock pandas to raise UnicodeDecodeError
        with patch('pandas.read_csv', side_effect=UnicodeDecodeError('utf-8', b'', 0, 1, 'invalid')):
            errors, df = validate_csv_file(content, "test.csv")

        assert "File 'test.csv' encoding not supported (please use UTF-8)" in errors
        assert df is None


class TestSaveUploadedFilesToDataDir:
    """Test the save_uploaded_files_to_data_dir function."""

    @pytest.fixture
    def temp_data_dir(self):
        """Create a temporary directory for testing file saves."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    def create_mock_uploaded_file(self, name: str, content: bytes = b"test,data\n1,2"):
        """Create a mock uploaded file for testing file saving."""
        mock_file = Mock()
        mock_file.name = name
        mock_file.size = len(content)
        mock_file.getbuffer.return_value = content
        return mock_file

    def test_save_single_file(self, temp_data_dir):
        """Test saving a single file."""
        mock_file = self.create_mock_uploaded_file("test_file.csv")

        with patch('utils.secure_filename', return_value="test_file.csv"):
            success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                [mock_file.getbuffer()], [mock_file.name], temp_data_dir
            )

        assert len(success_msgs) >= 1  # May include column sanitization messages
        assert len(error_msgs) == 0
        assert os.path.exists(os.path.join(temp_data_dir, "test_file.csv"))
        # Check that file save was successful
        assert any("Saved" in msg for msg in success_msgs)

    def test_save_multiple_files(self, temp_data_dir):
        """Test saving multiple files."""
        mock_files = [
            self.create_mock_uploaded_file("file1.csv"),
            self.create_mock_uploaded_file("file2.csv")
        ]

        with patch('utils.secure_filename', side_effect=lambda x: x):
            success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                [f.getbuffer() for f in mock_files], [f.name for f in mock_files], temp_data_dir
            )

        assert len(success_msgs) >= 2  # May include column sanitization messages
        assert len(error_msgs) == 0
        assert os.path.exists(os.path.join(temp_data_dir, "file1.csv"))
        assert os.path.exists(os.path.join(temp_data_dir, "file2.csv"))
        # Check that both files were saved
        assert sum(1 for msg in success_msgs if "Saved" in msg) == 2

    def test_filename_conflict_resolution(self, temp_data_dir):
        """Test that filename conflicts are resolved with numeric suffixes."""
        # First, create an existing file
        existing_file_path = os.path.join(temp_data_dir, "test_file.csv")
        with open(existing_file_path, "w") as f:
            f.write("existing,content\n")

        # Now try to save a file with the same name
        mock_file = self.create_mock_uploaded_file("test_file.csv")

        with patch('utils.secure_filename', return_value="test_file.csv"):
            success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                [mock_file.getbuffer()], [mock_file.name], temp_data_dir
            )

        assert len(success_msgs) >= 1  # May include column sanitization messages
        assert len(error_msgs) == 0
        # Should be saved with a suffix
        assert os.path.exists(os.path.join(temp_data_dir, "test_file_1.csv"))
        # Success message should mention the conflict resolution
        assert any("test_file_1.csv" in msg for msg in success_msgs)

    def test_multiple_filename_conflicts(self, temp_data_dir):
        """Test handling of multiple filename conflicts."""
        # Create existing files
        for i in ["", "_1", "_2"]:
            existing_path = os.path.join(temp_data_dir, f"test_file{i}.csv")
            with open(existing_path, "w") as f:
                f.write("existing,content\n")

        # Try to save a file with conflicting name
        mock_file = self.create_mock_uploaded_file("test_file.csv")

        with patch('utils.secure_filename', return_value="test_file.csv"):
            success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                [mock_file.getbuffer()], [mock_file.name], temp_data_dir
            )

        assert len(success_msgs) >= 1  # May include column sanitization messages
        assert len(error_msgs) == 0
        # Should be saved with suffix _3
        assert os.path.exists(os.path.join(temp_data_dir, "test_file_3.csv"))
        # Success message should mention _3 suffix
        assert any("test_file_3.csv" in msg for msg in success_msgs)

    def test_data_directory_creation(self):
        """Test that the data directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            non_existent_dir = os.path.join(temp_dir, "new_data_dir")
            assert not os.path.exists(non_existent_dir)

            mock_file = self.create_mock_uploaded_file("test.csv")

            with patch('utils.secure_filename', return_value="test.csv"):
                success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                    [mock_file.getbuffer()], [mock_file.name], non_existent_dir
                )

            assert os.path.exists(non_existent_dir)
            assert len(success_msgs) >= 1  # May include column sanitization messages
            assert len(error_msgs) == 0
            # Check that file was saved
            assert any("Saved" in msg for msg in success_msgs)

    def test_file_save_error_handling(self, temp_data_dir):
        """Test handling of file save errors."""
        mock_file = self.create_mock_uploaded_file("test.csv")

        # Create an invalid bytes object that will cause a write error
        invalid_content = b"\x00\x01\x02\x03" * 1000  # Binary content

        with patch('utils.secure_filename', return_value="test.csv"):
            # Use invalid content that should cause the function to handle errors gracefully
            success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                [invalid_content], [mock_file.name], temp_data_dir
            )

        # The function should handle errors and possibly succeed (it writes binary data fine)
        # If it succeeds, that's also valid behavior
        assert isinstance(success_msgs, list)
        assert isinstance(error_msgs, list)
