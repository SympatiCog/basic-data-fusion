"""
Styling constants and themes for the Query UI.

This module contains centralized styling definitions to ensure consistent
appearance across all query interface components.
"""

# Color scheme
COLORS = {
    'primary': '#007bff',
    'secondary': '#6c757d', 
    'success': '#28a745',
    'info': '#17a2b8',
    'warning': '#ffc107',
    'danger': '#dc3545',
    'light': '#f8f9fa',
    'dark': '#343a40',
    'longitudinal': '#28a745',  # Green for longitudinal data
    'cross_sectional': '#007bff'  # Blue for cross-sectional data
}

# Common spacing values
SPACING = {
    'xs': '5px',
    'sm': '10px', 
    'md': '20px',
    'lg': '30px',
    'xl': '40px'
}

# Component-specific styles
STYLES = {
    'logo': {
        'width': '100%',
        'height': 'auto',
        'maxWidth': '250px'
    },
    
    'upload_area': {
        'width': '100%',
        'height': '60px',
        'lineHeight': '60px',
        'borderWidth': '1px',
        'borderStyle': 'dashed',
        'borderRadius': '5px',
        'textAlign': 'center',
        'margin': '10px'
    },
    
    'section_spacing': {
        'marginTop': SPACING['md'],
        'marginBottom': SPACING['md']
    },
    
    'card_body_spacing': {
        'marginTop': SPACING['md']
    },
    
    'filter_spacing': {
        'marginTop': SPACING['xs']
    },
    
    'hidden': {
        'display': 'none'
    },
    
    'enwiden_checkbox_wrapper': {
        'display': 'none', 
        'marginTop': SPACING['sm']
    }
}

# Bootstrap classes commonly used
CLASSES = {
    'flex_center': "d-flex justify-content-center align-items-center",
    'text_muted': "card-text text-muted",
    'button_spacing': "g-2",
    'full_width': "w-100",
    'margin_bottom': "mb-3",
    'margin_top': "mt-2",
    'text_start': "text-start"
}

# Icon mappings
ICONS = {
    'upload': "bi bi-upload me-2",
    'download': "bi bi-download me-2", 
    'chevron_down': "bi bi-chevron-down ms-2",
    'info_circle': "bi bi-info-circle me-2",
    'check_circle': "bi bi-check-circle me-2",
    'cloud_upload': "bi bi-cloud-upload me-2"
}

# Modal sizes
MODAL_SIZES = {
    'small': 'sm',
    'medium': 'md', 
    'large': 'lg',
    'extra_large': 'xl'
}