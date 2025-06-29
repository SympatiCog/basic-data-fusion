"""
File handling module for Basic Data Fusion.

This module provides comprehensive file handling capabilities including
secure uploads, CSV processing, path utilities, and file validation.
"""

# Security functions
from .security import (
    secure_filename,
    sanitize_column_names,
    validate_file_path,
    check_file_extension,
    validate_file_size,
    detect_malicious_content,
    generate_safe_filename
)

# CSV utilities
from .csv_utils import (
    validate_csv_file,
    process_csv_file,
    scan_csv_files,
    get_csv_info,
    validate_csv_structure
)

# Upload handling
from .upload import (
    DuplicateFileInfo,
    FileActionChoice,
    UploadResult,
    check_for_duplicate_files,
    save_uploaded_files_to_data_dir,
    validate_upload_request,
    get_upload_summary,
    cleanup_failed_uploads
)

# Path utilities
from .path_utils import (
    shorten_path,
    get_directory_mtime,
    ensure_safe_path,
    create_safe_directory,
    list_csv_files,
    get_relative_path,
    is_safe_filename,
    normalize_path_separators,
    get_file_size_human_readable,
    cleanup_empty_directories
)

__all__ = [
    # Security functions
    'secure_filename',
    'sanitize_column_names',
    'validate_file_path',
    'check_file_extension',
    'validate_file_size',
    'detect_malicious_content',
    'generate_safe_filename',
    
    # CSV utilities
    'validate_csv_file',
    'process_csv_file',
    'scan_csv_files',
    'get_csv_info',
    'validate_csv_structure',
    
    # Upload handling
    'DuplicateFileInfo',
    'FileActionChoice',
    'UploadResult',
    'check_for_duplicate_files',
    'save_uploaded_files_to_data_dir',
    'validate_upload_request',
    'get_upload_summary',
    'cleanup_failed_uploads',
    
    # Path utilities
    'shorten_path',
    'get_directory_mtime',
    'ensure_safe_path',
    'create_safe_directory',
    'list_csv_files',
    'get_relative_path',
    'is_safe_filename',
    'normalize_path_separators',
    'get_file_size_human_readable',
    'cleanup_empty_directories',
]

# Version info
__version__ = "1.0.0"