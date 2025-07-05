"""
File upload handling for Basic Data Fusion.

This module provides functions for handling file uploads, duplicate detection,
and secure file saving operations.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from core.exceptions import FileProcessingError

# Exception aliases for this module
FileUploadError = FileProcessingError
FileHandlingError = FileProcessingError
from .security import secure_filename, generate_safe_filename
from .csv_utils import process_csv_file


@dataclass
class DuplicateFileInfo:
    """Information about a duplicate file conflict."""
    original_filename: str
    safe_filename: str
    existing_path: str
    content: bytes


@dataclass
class FileActionChoice:
    """User's choice for handling a duplicate file."""
    action: str  # 'replace', 'rename', or 'cancel'
    new_filename: Optional[str] = None  # Used when action is 'rename'


@dataclass
class UploadResult:
    """Result of file upload operation."""
    success_messages: List[str]
    error_messages: List[str]
    processed_files: List[str]  # List of successfully processed filenames
    failed_files: List[str]     # List of failed filenames


def check_for_duplicate_files(
    file_contents: List[bytes], 
    filenames: List[str], 
    data_dir: str
) -> Tuple[List[DuplicateFileInfo], List[int]]:
    """
    Check for duplicate files without saving them.
    
    Args:
        file_contents: List of file contents as bytes
        filenames: List of original filenames
        data_dir: Target directory to check for duplicates
        
    Returns:
        Tuple of (list of duplicate file info, list of indices of non-duplicate files)
        
    Raises:
        FileHandlingError: If directory operations fail
    """
    try:
        # Ensure directory exists
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        
        duplicates = []
        non_duplicate_indices = []

        for i, (content, filename) in enumerate(zip(file_contents, filenames)):
            safe_filename = secure_filename(filename)
            file_path = Path(data_dir) / safe_filename

            if file_path.exists():
                duplicates.append(DuplicateFileInfo(
                    original_filename=filename,
                    safe_filename=safe_filename,
                    existing_path=str(file_path),
                    content=content
                ))
            else:
                non_duplicate_indices.append(i)

        return duplicates, non_duplicate_indices
    
    except Exception as e:
        error_msg = f"Error checking for duplicate files: {e}"
        raise FileHandlingError(error_msg, operation="duplicate_check")


def save_uploaded_files_to_data_dir(
    file_contents: List[bytes],
    filenames: List[str],
    data_dir: str,
    duplicate_actions: Optional[Dict[str, FileActionChoice]] = None,
    sanitize_columns: bool = True,
    config_params: Optional[Dict] = None
) -> UploadResult:
    """
    Save uploaded file contents to the data directory with column name sanitization.
    
    Args:
        file_contents: List of file contents as bytes
        filenames: List of original filenames
        data_dir: Target directory
        duplicate_actions: Dict mapping original filenames to user's action choices for duplicates
        sanitize_columns: Whether to sanitize column names in CSV files
        config_params: Configuration parameters for column validation and composite ID creation
        
    Returns:
        UploadResult with success/error messages and processed file lists
        
    Raises:
        FileUploadError: If upload operation fails critically
    """
    try:
        success_messages = []
        error_messages = []
        processed_files = []
        failed_files = []
        
        # Ensure directory exists
        Path(data_dir).mkdir(parents=True, exist_ok=True)

        for content, filename in zip(file_contents, filenames):
            try:
                # Process this individual file
                result = _save_single_file(
                    content, filename, data_dir, duplicate_actions, sanitize_columns, config_params
                )
                
                if result['success']:
                    success_messages.extend(result['messages'])
                    processed_files.append(result['final_filename'])
                else:
                    error_messages.extend(result['messages'])
                    failed_files.append(filename)
            
            except Exception as e:
                error_msg = f"❌ Failed to save '{filename}': {str(e)}"
                error_messages.append(error_msg)
                failed_files.append(filename)

        return UploadResult(
            success_messages=success_messages,
            error_messages=error_messages,
            processed_files=processed_files,
            failed_files=failed_files
        )
    
    except Exception as e:
        error_msg = f"Critical error in file upload operation: {e}"
        raise FileUploadError(error_msg, details={
            'data_dir': data_dir,
            'num_files': len(filenames)
        })


def _save_single_file(
    content: bytes,
    filename: str,
    data_dir: str,
    duplicate_actions: Optional[Dict[str, FileActionChoice]],
    sanitize_columns: bool,
    config_params: Optional[Dict] = None
) -> Dict[str, any]:
    """
    Save a single file with duplicate handling.
    
    Args:
        content: File content as bytes
        filename: Original filename
        data_dir: Target directory
        duplicate_actions: User choices for duplicate handling
        sanitize_columns: Whether to sanitize column names
        config_params: Configuration parameters for column validation and composite ID creation
        
    Returns:
        Dictionary with 'success', 'messages', and 'final_filename'
    """
    messages = []
    
    try:
        safe_filename = secure_filename(filename)
        file_path = Path(data_dir) / safe_filename
        final_filename = safe_filename

        # Handle filename conflicts based on user choice
        if file_path.exists() and duplicate_actions and filename in duplicate_actions:
            action_choice = duplicate_actions[filename]

            if action_choice.action == 'cancel':
                # Skip this file
                return {
                    'success': False,
                    'messages': [f"⏭️ Skipped '{filename}' (user cancelled)"],
                    'final_filename': ''
                }
            elif action_choice.action == 'replace':
                # Use the existing file path (will overwrite)
                messages.append(f"🔄 Replaced existing file '{safe_filename}'")
            elif action_choice.action == 'rename' and action_choice.new_filename:
                # Use the new filename provided by user
                new_safe_filename = secure_filename(action_choice.new_filename)
                file_path = Path(data_dir) / new_safe_filename
                final_filename = new_safe_filename

                # Check if the new name also conflicts
                if file_path.exists():
                    return {
                        'success': False,
                        'messages': [f"❌ New filename '{new_safe_filename}' also already exists"],
                        'final_filename': ''
                    }

                messages.append(f"📝 Saved '{filename}' as '{new_safe_filename}'")
        
        elif file_path.exists():
            # Fallback to auto-rename if no user choice provided
            existing_files = [f.name for f in Path(data_dir).glob('*.csv')]
            final_filename = generate_safe_filename(safe_filename, existing_files)
            file_path = Path(data_dir) / final_filename
            messages.append(f"File '{filename}' already exists, saved as '{final_filename}'")

        # Process the CSV content
        df, process_success_msgs, process_error_msgs = process_csv_file(
            content, filename, sanitize_columns, config_params
        )
        
        if process_error_msgs:
            return {
                'success': False,
                'messages': process_error_msgs,
                'final_filename': ''
            }
        
        # Save the processed DataFrame
        df.to_csv(file_path, index=False)
        
        # Create success message
        size_msg = f"({len(content):,} bytes)"
        if not any(msg.startswith("🔄") or msg.startswith("📝") for msg in messages):
            messages.append(f"✅ Saved '{filename}' as '{final_filename}' {size_msg}")
        
        # Add processing messages
        messages.extend(process_success_msgs)
        
        return {
            'success': True,
            'messages': messages,
            'final_filename': final_filename
        }
    
    except Exception as e:
        return {
            'success': False,
            'messages': [f"❌ Failed to save '{filename}': {str(e)}"],
            'final_filename': ''
        }


def validate_upload_request(
    file_contents: List[bytes],
    filenames: List[str],
    max_files: int = 10,
    max_total_size_mb: int = 500
) -> Tuple[bool, List[str]]:
    """
    Validate file upload request before processing.
    
    Args:
        file_contents: List of file contents as bytes
        filenames: List of original filenames
        max_files: Maximum number of files allowed
        max_total_size_mb: Maximum total size in megabytes
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    try:
        # Check number of files
        if len(file_contents) > max_files:
            errors.append(f"Too many files ({len(file_contents)}). Maximum allowed: {max_files}")
        
        if len(file_contents) != len(filenames):
            errors.append("Mismatch between number of file contents and filenames")
        
        # Check total size
        total_size = sum(len(content) for content in file_contents)
        max_total_size_bytes = max_total_size_mb * 1024 * 1024
        
        if total_size > max_total_size_bytes:
            total_size_mb = total_size / (1024 * 1024)
            errors.append(f"Total upload size ({total_size_mb:.1f}MB) exceeds limit ({max_total_size_mb}MB)")
        
        # Check individual file names
        for filename in filenames:
            if not filename:
                errors.append("Empty filename detected")
                continue
            
            if not filename.lower().endswith('.csv'):
                errors.append(f"File '{filename}' is not a CSV file")
        
        # Check for duplicate filenames in the upload
        if len(set(filenames)) != len(filenames):
            duplicates = [f for f in filenames if filenames.count(f) > 1]
            errors.append(f"Duplicate filenames in upload: {', '.join(set(duplicates))}")
        
        return len(errors) == 0, errors
    
    except Exception as e:
        errors.append(f"Error validating upload request: {e}")
        return False, errors


def get_upload_summary(upload_result: UploadResult) -> Dict[str, any]:
    """
    Generate a summary of the upload operation.
    
    Args:
        upload_result: Result of the upload operation
        
    Returns:
        Dictionary with upload summary statistics
    """
    return {
        'total_files_attempted': len(upload_result.processed_files) + len(upload_result.failed_files),
        'successful_uploads': len(upload_result.processed_files),
        'failed_uploads': len(upload_result.failed_files),
        'success_rate': len(upload_result.processed_files) / max(1, len(upload_result.processed_files) + len(upload_result.failed_files)) * 100,
        'processed_files': upload_result.processed_files,
        'failed_files': upload_result.failed_files,
        'total_success_messages': len(upload_result.success_messages),
        'total_error_messages': len(upload_result.error_messages)
    }


def cleanup_failed_uploads(data_dir: str, failed_files: List[str]) -> List[str]:
    """
    Clean up any partially uploaded files that failed during processing.
    
    Args:
        data_dir: Directory containing uploaded files
        failed_files: List of filenames that failed to upload
        
    Returns:
        List of cleanup messages
    """
    cleanup_messages = []
    
    try:
        for filename in failed_files:
            safe_filename = secure_filename(filename)
            file_path = Path(data_dir) / safe_filename
            
            if file_path.exists():
                try:
                    file_path.unlink()  # Delete the file
                    cleanup_messages.append(f"Cleaned up partial file: {safe_filename}")
                except Exception as e:
                    cleanup_messages.append(f"Failed to clean up {safe_filename}: {e}")
    
    except Exception as e:
        cleanup_messages.append(f"Error during cleanup: {e}")
    
    return cleanup_messages