# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Dash-based data browser application for laboratory research data. The app allows researchers to filter, query, merge, and export CSV datasets using an interactive multi-page web interface backed by DuckDB for efficient SQL queries. The application features a modular architecture with comprehensive security, state management, and data processing capabilities.

## Key Architecture

### Core Application
- **Main Application**: `app.py` - Dash multi-page application with centralized routing
- **Pages**: Multi-page architecture with dedicated functionality:
  - `pages/query.py` - Main data query and filtering interface (path: `/`) - **Currently in transition to modular architecture**
  - `pages/import.py` - Data import and file upload functionality (path: `/import`)
  - `pages/settings.py` - Configuration management interface (path: `/settings`)
  - `pages/profiling.py` / `pages/02_📊_Data_Profiling.py` - Data profiling and exploration (path: `/profiling`)
  - `pages/plotting.py` - Interactive data visualization and plotting (path: `/plotting`)
  - `pages/onboarding.py` - User onboarding and tutorial interface (path: `/onboarding`)

### Modular Architecture
The application has been refactored into a modular architecture with specialized modules:

- **Core Infrastructure** (`core/`):
  - `config.py` - Modular configuration management with split config classes
  - `database.py` - Database connection management and caching
  - `exceptions.py` - Custom exception hierarchy for error handling
  - `logging_config.py` - Centralized logging configuration

- **Data Handling** (`data_handling/`):
  - `merge_strategy.py` - Flexible merge strategies for cross-sectional/longitudinal data
  - `metadata.py` - Data structure detection, metadata management, and session value extraction

- **Query Processing** (`query/`):
  - **Core Query Logic**:
    - `query_builder.py` - SQL query construction
    - `query_factory.py` - Query generation factory pattern
    - `query_parameters.py` - Parameter validation and sanitization
    - `query_secure.py` - Security-focused query generation with injection prevention
  - **Modular Query Interface** (Phase 2.2 Complete):
    - `ui/layout.py` - Modular query page layout with component-based structure
    - `ui/components.py` - Reusable UI components (demographic filters, phenotypic filters, export cards, modals)
    - `ui/styles.py` - Centralized styling constants and Bootstrap integration
    - `callbacks/data_loading.py` - ✅ Data status, table info, and column selection callbacks (complete)
    - `callbacks/filters.py` - ✅ **ALL major filter callbacks extracted** (Phase 2.2 complete)
      - `manage_phenotypic_filters()` - Complex state management (160+ lines)
      - `render_phenotypic_filters()` - UI rendering logic (180+ lines) 
      - `update_live_participant_count()` - Live count updates (103+ lines)
      - `update_phenotypic_session_notice()` - Session notice management
    - `callbacks/export.py` - 🔄 Data export and generation callbacks (Phase 2.3 target)
    - `callbacks/state.py` - ❌ State management callbacks (Phase 2.4 planned)
    - `helpers/ui_builders.py` - UI generation helper functions (Phase 3 planned)
    - `helpers/data_formatters.py` - ✅ Data formatting utilities (convert_phenotypic_to_behavioral_filters implemented)
    - `helpers/validation.py` - Input validation helpers (Phase 3 planned)

- **File Operations** (`file_handling/`):
  - `csv_utils.py` - CSV file validation and processing
  - `path_utils.py` - Safe path handling, validation, and intelligent path shortening
  - `security.py` - File security and path traversal prevention
  - `upload.py` - Secure file upload handling

- **Analysis & Statistics** (`analysis/`):
  - `demographics.py` - Demographic data analysis, multisite detection, and study site value extraction
  - `export.py` - Data export formatting and processing
  - `filtering.py` - Advanced filtering logic
  - `statistics.py` - Statistical analysis functions, numeric type checking, and data profiling

### State Management System
- **StateManager** (`state_manager.py`) - Centralized state management with multiple backend support
- **Session Management** (`session_manager.py`) - User session handling and persistence
- **State Backends** (`state_backends.py`) - Pluggable state storage backends (client, Redis, database)
- **State Utilities** (`state_utils.py`) - Helper functions for state operations
- **✅ PHASE 4 COMPLETE**: **Consolidated Query State** (`query/state/`) - Unified state management:
  - `models.py` - Typed state schema with QueryPageState dataclass
  - `helpers.py` - State manipulation utilities and backward compatibility
  - `consolidated-query-state-store` - Single store replacing 19+ individual stores
  - Synchronization callbacks for seamless migration from fragmented stores

### Security Infrastructure
- **Security Utils** (`security_utils.py`) - Security validation and sanitization functions
- **Comprehensive Security Testing** - Extensive security test coverage
- **SQL Injection Prevention** - Parameterized queries and input validation
- **Path Traversal Protection** - Safe file operations and path validation

### Legacy Compatibility
- **Backward Compatibility** (`utils.py`) - Maintains compatibility with legacy code by re-exporting modular APIs
- **Configuration Management**: Centralized configuration system using TOML files and dataclasses
- **Data Storage**: `data/` directory contains CSV files with research data
- **Flexible Merge Strategy**: Auto-detects cross-sectional vs longitudinal data structures
- **Demographics Base**: `demographics.csv` serves as the primary table for LEFT JOINs
- **Query Engine**: DuckDB provides in-memory SQL processing for fast data operations

## Development Commands

```bash
# Install dependencies
uv sync

# Run the Dash application
python app.py

# Application will be available at http://127.0.0.1:8050/

# Start Jupyter Lab for data analysis
jupyter lab

# Run tests
pytest

# Run tests with coverage
pytest --cov

# Run linting and type checking
ruff check
mypy .
```

## Testing Infrastructure

The project includes comprehensive testing with 21+ test files covering all aspects of the application:

### Test Directory Structure
- **Test Directory**: `tests/` contains all test files
- **Fixtures**: `tests/fixtures/` provides sample datasets for testing:
  - `cross_sectional/` - Test data for cross-sectional analysis
  - `longitudinal/` - Test data for longitudinal analysis  
  - `rockland/` - Additional test datasets
  - `medium_priority/` - Complex test scenarios and edge cases

### Core Test Coverage
- **Configuration Management**: 
  - `test_config.py` - Configuration system testing
  - `test_config_security_critical.py` - Security-focused config tests
- **Core Functionality**: 
  - `test_core.py` - Core application functionality
  - `test_data_processing.py` - Data processing pipeline
  - `test_data_processing_pipeline.py` - End-to-end data processing
  - `test_data_merge_comprehensive.py` - Comprehensive merge strategy testing
- **Security Testing**:
  - `test_security_critical.py` - Critical security validations
  - `test_security_fixes.py` - Security fix verification
  - `test_sql_injection_critical.py` - SQL injection prevention
  - `test_secure_query_generation.py` - Secure query building
  - `test_secure_query_integration.py` - Integrated security testing
- **User Interface & Integration**:
  - `test_dash_callbacks.py` - Dash callback functionality
  - `test_dash_ui_integration.py` - UI integration testing
  - `test_integration.py` - System integration tests
- **Data Operations**:
  - `test_file_upload.py` - File upload and validation
  - `test_sql_generation.py` - SQL query generation
  - `test_data_export_formatting.py` - Data export functionality
  - `test_plotting_visualization.py` - Visualization and plotting
- **System Components**:
  - `test_state_manager.py` - State management system
  - `test_error_handling_feedback.py` - Error handling and user feedback
  - `test_callback_registration.py` - Enhanced callback registration system (13 tests)

### Quality Tools
- **Testing Framework**: pytest with comprehensive fixtures and parameterized tests
- **Coverage Reporting**: pytest-cov for test coverage analysis
- **Code Quality**: ruff linting and mypy type checking
- **Security Focus**: Dedicated security test suites with critical security validations

## Configuration Management

The application uses a sophisticated modular configuration system with the following components:

### Configuration Architecture
- `config.toml` - Main configuration file with all settings
- `config_manager.py` - Singleton pattern for centralized config management
- `core/config.py` - Modular configuration classes with split concerns:
  - `DataConfig` - Data handling and file operations
  - `UIConfig` - User interface settings and display options
  - `SecurityConfig` - Security settings and validation rules
  - `Config` - Main configuration class that combines all config modules
- `utils.py` - Backward compatibility layer for legacy config access

### Configurable Settings
- **Data Directory**: Location of CSV files (`data_dir`)
- **Demographics File**: Primary demographics file name (`demographics_file`)
- **Column Mapping**: Configurable column names for different data formats:
  - `primary_id_column` - Subject identifier (default: "ursi")
  - `session_column` - Session identifier for longitudinal data (default: "session_num")  
  - `composite_id_column` - Composite ID column name (default: "customID")
  - `age_column` - Age data column name (default: "age")
  - `sex_column` - Sex/gender column name (default: "sex")
- **Default Filters**: Age ranges and sex selection defaults
- **Sex Mapping**: String-to-numeric mapping for sex categories
- **Display Settings**: Maximum rows to display in tables

### Configuration Web Interface
The Settings page (`/settings`) provides a comprehensive interface for:
- Modifying all configuration parameters
- Real-time configuration preview
- Import/export of configuration files (TOML/JSON)
- Reset to default settings
- Automatic validation and error handling

## Data Structure

The application automatically detects dataset structure and adapts merge strategy:

### Cross-sectional Data
- Uses primary ID column for merging
- No session information required
- Simple, direct merging across tables

### Longitudinal Data  
- Detects presence of both primary ID and session columns
- Automatically creates composite ID for precise session-level merging
- Enables session-specific filtering and analysis
- Supports both long and wide format exports

## Data Import System

The Import page (`/import`) provides:
- Drag-and-drop file upload interface
- Multi-file CSV validation
- Real-time upload status and error reporting
- Data summary and merge strategy detection
- Integration with configuration system

## Core Functions

### Configuration Management
- `get_config()`: Singleton function to get current configuration
- `refresh_config()`: Force refresh of global configuration instance
- `Config.save_config()`: Save current configuration to TOML file
- `Config.load_config()`: Load configuration from TOML file
- `core.config` module: Modular configuration classes for different concerns

### Data Processing (`data_handling/`)
- `get_table_info()`: Scans data directory, detects structure, returns metadata (cached 10 minutes)
- `get_unique_session_values()`: Extracts unique session values for longitudinal data filtering
- `MergeKeys`: Encapsulates merge column information and dataset structure
- `FlexibleMergeStrategy`: Auto-detects and handles cross-sectional vs longitudinal data
- `merge_strategy.py`: Advanced merge strategies for different data structures
- `metadata.py`: Data structure detection, table information management, and session value extraction

### Query Processing (`query/`)
- **Core Query Logic**:
  - `query_secure.py`: Security-focused query generation with injection prevention
  - `query_builder.py`: SQL query construction with parameterization
  - `query_factory.py`: Factory pattern for query generation
  - `query_parameters.py`: Parameter validation and sanitization
  - `generate_base_query_logic()`: Creates FROM/JOIN/WHERE clauses with flexible merge keys
  - `generate_data_query()`: Builds full SELECT query for data export
  - `generate_count_query()`: Builds COUNT query for participant matching

- **Modular Query Interface** (Partially Implemented):
  - `ui/layout.layout`: Component-based query page layout using modular UI functions
  - `ui/components.create_*_card()`: Individual UI component builders for filters, export, results
  - `callbacks/register_all_callbacks()`: Centralized callback registration system
  - **Current Status**: Infrastructure exists but not yet integrated into main application

### File Operations (`file_handling/`)
- `csv_utils.py`: CSV file validation and processing
- `upload.py`: Secure file upload handling
- `security.py`: File security and path traversal prevention
- `path_utils.py`: Safe path handling, validation, and intelligent path shortening
- `shorten_path()`: Smart path truncation that preserves meaningful components
- `validate_csv_file()`: Comprehensive CSV file validation
- `save_uploaded_files_to_data_dir()`: Secure file saving with validation

### Database Management (`core/`)
- `database.py`: Database connection management and caching
- `get_db_connection()`: Cached DuckDB connection management
- Connection pooling and transaction management

### State Management
- `StateManager`: Centralized state management with pluggable backends
- `state_backends.py`: Multiple storage backends (client, Redis, database)
- `session_manager.py`: User session handling and persistence
- `state_utils.py`: State operation helper functions

### Analysis & Statistics (`analysis/`)
- `demographics.py`: Demographic data analysis functions
- `has_multisite_data()`: Detects multisite/multi-center study configurations
- `get_study_site_values()`: Extracts unique study site values with flexible format handling
- `export.py`: Data export formatting and processing
- `filtering.py`: Advanced filtering logic
- `statistics.py`: Statistical analysis functions
- `is_numeric_dtype()`: Checks if data type represents numeric values
- `is_numeric_column()`: Validates if column contains numeric data
- `enwiden_longitudinal_data()`: Pivots longitudinal data from long to wide format

## Application Features

### Query Interface
- Real-time participant count updates
- Dynamic demographic and phenotypic filters
- Session-based filtering for longitudinal data
- Export options (CSV, long/wide format)
- Merge strategy visualization

#### Query Interface Architecture Status
- **Current Implementation**: Monolithic `pages/query.py` (2,091 lines) with all callbacks and layout
- **Refactoring Progress**: 
  - ✅ **Complete**: Modular UI components (`query/ui/`) with layout, components, and styles
  - 🔄 **Partial**: Callback extraction (~40% complete) - data loading and basic filters implemented
  - ❌ **Pending**: Layout migration, major callback extraction (phenotypic filters, export, state management)
  - ❌ **Pending**: Helper function implementation and app integration
- **Target Architecture**: Component-based layout with callback registration system

### Phenotypic Filtering System
- Table and column selection with dynamic options
- Support for numeric (range) and categorical (selection) filters
- Real-time filter validation and participant count updates
- Complex multi-table filtering logic
- **Fixed**: Column selection bug where first filter remained disabled after table selection
- **Enhanced**: Improved pattern-matching callback handling for dynamic filter creation

### State Management
- **Current System**: 19 individual Dash stores for different UI components
- **Architecture**: 
  - Dash stores for session persistence
  - Real-time updates across components
  - Prevention of callback interference
  - Smart initialization and refresh patterns
- **Planned Consolidation**: Single consolidated state store with typed state management classes (outlined in query refactoring plan but not yet implemented)

### Data Visualization and Plotting
The Plotting page (`/plotting`) provides:
- **Interactive Plot Types**: Scatter plots, histograms, box plots, violin plots, density heatmaps
- **Dynamic Configuration**: Real-time column selection based on data types (numeric/categorical)
- **Cross-Filtering**: Select points on plots to filter the data table below with precise index matching
- **Data Upload**: Drag-and-drop CSV upload or automatic data loading from Query page
- **Export Functionality**: Export selected data points to CSV
- **Advanced Plot Features**: 
  - Configurable aesthetics (color, size, faceting)
  - Interactive selection tools (box select, lasso select)
  - Automatic handling of missing values and data filtering
  - Smart size adjustments for negative values in scatter plots
- **Robust Error Handling**: Comprehensive validation and user-friendly error messages

#### Cross-Filtering Implementation
- Uses filtered dataframe storage (`filtered-plot-df-store`) to ensure selected plot points match correct table rows
- Handles data filtering (NaN removal) transparently for accurate point-to-row mapping
- Real-time updates between plot selections and data table display
- **Recent Fix**: Resolved index mismatch issue where filtered plot data indices didn't align with original dataframe rows

## Security Infrastructure

The application implements comprehensive security measures throughout the architecture:

### Security Components
- **Security Utils** (`security_utils.py`) - Central security validation and sanitization
- **Query Security** (`query/query_secure.py`) - Parameterized queries and SQL injection prevention
- **File Security** (`file_handling/security.py`) - Path traversal protection and safe file operations
- **Path Validation** (`file_handling/path_utils.py`) - Secure path handling and normalization

### Security Features
- **SQL Injection Prevention**: All database queries use parameterized statements
- **Path Traversal Protection**: File operations validate and normalize paths
- **Input Sanitization**: Comprehensive validation of user inputs
- **Configuration Security**: Secure configuration loading and validation
- **File Upload Security**: Safe file handling with content validation

### Security Testing
- Dedicated security test suites with critical security validations
- SQL injection prevention testing
- Path traversal attack prevention
- Configuration security testing
- File upload security validation

## Query Interface Refactoring Status

The main query interface (`pages/query.py`) is undergoing a major architectural refactoring to improve maintainability and modularity. **Phase 2.2 has been completed successfully!**

### Phase 1: Foundation & Structure ✅ **COMPLETE**
- **Modular UI Package**: Complete `query/ui/` package with:
  - `layout.py` - Full component-based layout definition
  - `components.py` - All UI components (demographic filters, phenotypic filters, export cards, modals)
  - `styles.py` - Centralized styling and Bootstrap integration
- **Callback Registration System**: Centralized callback management in `query/callbacks/__init__.py`

### Phase 2: Callback Decomposition Progress
- ✅ **Phase 2.1 COMPLETE**: Data Loading Callbacks - All moved to `query/callbacks/data_loading.py`
- ✅ **Phase 2.2 COMPLETE**: Filter Management Callbacks - **MAJOR MILESTONE ACHIEVED**
  - **Major Callbacks Extracted** (664 lines moved from `pages/query.py`):
    - `manage_phenotypic_filters()` - Complex state management (160+ lines)
    - `render_phenotypic_filters()` - UI rendering logic (180+ lines)
    - `update_live_participant_count()` - Live count updates (103+ lines)
    - `update_phenotypic_session_notice()` - Session notice management
  - **Impact**: Reduced `pages/query.py` from 1,385 to 932 lines (**33% reduction**)
  - **All phenotypic filter functionality** now properly modularized
- ✅ **Phase 2.3 COMPLETE**: Data Export Callbacks - All moved to `query/callbacks/export.py`
- ✅ **Phase 2.4 COMPLETE**: State Management Callbacks - **PHASE 2 COMPLETION MILESTONE**
  - **State Management Callbacks Extracted** (576+ lines moved from `pages/query.py`):
    - `update_study_site_store()` and `update_session_selection_store()` - Store updates
    - `restore_*_value()` functions - All state restoration callbacks
    - `save_all_filter_states()` - State persistence across navigation
    - `update_enwiden_checkbox_visibility()` - UI state control
    - `toggle_export_modal()` and `export_query_parameters()` - Export parameter management
    - `toggle_import_modal()`, `handle_file_upload()`, `apply_imported_parameters()` - Import functionality
  - **Impact**: Reduced `pages/query.py` from 932 to 356 lines (**62% additional reduction**)
  - **All state management functionality** now properly modularized

### Current Status 🎯 **PHASE 3 COMPLETE** ✅
- **File Reduction**: `pages/query.py` reduced from **1,385 to 271 lines (80% total reduction)**
- **Final Reduction**: Additional 85 lines removed in Phase 3 (24% reduction from Phase 2 end state)
- **Modular Coverage**: All major callback functionality and helper functions extracted and modularized
- **Module Distribution**:
  - `query/callbacks/data_loading.py` - Data status and table management
  - `query/callbacks/filters.py` - All filter management (664 lines)
  - `query/callbacks/export.py` - Data generation and export (563 lines)
  - `query/callbacks/state.py` - State management and persistence (653 lines)
  - `query/helpers/ui_builders.py` - Query details UI building functions
- **Layout Integration**: `query/ui/layout.py` successfully integrated and used by main application
- **Functionality**: All existing behavior preserved with improved organization
- **Architecture**: Complete callback registration system with robust modular structure

### Critical Fixes Applied ✅
During Phase 2 testing, several critical issues were identified and resolved:

**TOML Import/Export Fixes:**
- **Issue**: `'dict' object has no attribute 'strip'` error during TOML parameter import
- **Root Cause**: Data type mismatch in `query/callbacks/state.py` - dictionary stored instead of string
- **Solution**: Fixed line 391 to store `file_content` (string) instead of `imported_data` (dict)
- **Result**: TOML query parameter import/export now works correctly

**Phenotypic Filter Stability:**
- **Issue**: Column selections immediately reset after user input + excessive callback loops
- **Root Cause**: Problematic state synchronization logic + multiple callbacks triggering simultaneously
- **Solutions**: 
  - Removed state synchronization code causing column selection resets
  - Added `prevent_initial_call=True` to reduce callback loops
  - Changed logging from INFO to DEBUG level to reduce terminal noise
- **Result**: Phenotypic filters now work reliably with persistent column selections

**Performance Optimization:**
- Reduced redundant callback processing during bulk state changes
- Minimized logging verbosity for better development experience
- Improved callback registration system stability

### Phase 3 Achievements ✅ **COMPLETE**
1. ✅ **Layout Migration**: `query/ui/layout.py` successfully integrated into main application
2. ✅ **Helper Function Extraction**: Query details UI building functions extracted to `query/helpers/ui_builders.py`
3. ✅ **Final Integration**: Complete transition from monolithic to modular architecture achieved

### Phase 3 Results Summary
- **Helper Functions Extracted**: Large `populate_query_details_content()` callback (100+ lines) refactored into modular helper functions
- **UI Builder Functions**: 6 specialized UI building functions created in `query/helpers/ui_builders.py`:
  - `build_file_information_section()` - File metadata display
  - `build_user_notes_section()` - User notes display
  - `build_cohort_filters_section()` - Cohort filter summary
  - `build_phenotypic_filters_section()` - Phenotypic filter summary
  - `build_export_selection_section()` - Export selection summary
  - `build_query_details_content()` - Complete modal content builder
- **Code Reduction**: Additional 85-line reduction (24%) in `pages/query.py`
- **Total Achievement**: 80% reduction from original 1,385 lines to final 271 lines

### Current Workflow ✅ **FULLY MODULAR**
When working on query interface features:
1. **For Filter Changes**: Use modular callbacks in `query/callbacks/filters.py` ✅
2. **For Export Changes**: Use modular callbacks in `query/callbacks/export.py` ✅
3. **For State Changes**: Use modular callbacks in `query/callbacks/state.py` ✅
4. **For Data Loading**: Use modular callbacks in `query/callbacks/data_loading.py` ✅
5. **For UI Changes**: Use existing modular components in `query/ui/components.py` ✅
6. **For Layout Changes**: Use modular layout in `query/ui/layout.py` ✅
7. **For UI Building**: Use helper functions in `query/helpers/ui_builders.py` ✅
8. **For New Callbacks**: Add to appropriate module in `query/callbacks/` and register in `__init__.py` ✅

### Phase 4: State Consolidation ✅ **COMPLETE**
- ✅ **Consolidated Query State**: Unified state management in `query/state/` 
  - `models.py` - Typed state schema with QueryPageState dataclass
  - `helpers.py` - State manipulation utilities and backward compatibility
  - `consolidated-query-state-store` - Single store replacing 19+ individual stores
  - Synchronization callbacks for seamless migration from fragmented stores

### Phase 5: Integration & Testing ✅ **COMPLETE**
- ✅ **Phase 5.1: Main Application Integration**
  - **Direct Page Registration**: Query page registered directly in `app.py` using `query.ui.layout.layout`
  - **Centralized Callback Registration**: All callbacks registered via `register_all_callbacks(app)` in app startup
  - **Final Migration**: All remaining callbacks moved from `pages/query.py` to modular system
  - **Minimal Compatibility Layer**: `pages/query.py` reduced from 201 to 41 lines (80% reduction)
  - **Total Code Reduction**: From original 2,429 lines to final 41 lines (**98.3% reduction achieved**)

- ✅ **Phase 5.2: Enhanced Callback Registration System**
  - **Performance Monitoring**: Registration timing (2.25ms for 36 callbacks) with per-module breakdown
  - **Error Handling**: Graceful partial failure recovery and complete failure detection
  - **Statistics Tracking**: Detailed registration metrics and module validation
  - **Developer Experience**: Verbose mode, utility functions, and comprehensive documentation
  - **Testing Infrastructure**: 13 comprehensive tests with 100% pass rate
  - **Enterprise-Grade Reliability**: Robust duplicate prevention and error logging

### Refactoring Status: **FULLY COMPLETE** 🎯
The query interface refactoring is now **100% complete** with enterprise-grade modular architecture:
- **98.3% Code Reduction**: From 2,429 lines to 41 lines 
- **36 Callbacks**: Distributed across 4 specialized modules
- **Zero Regression**: All functionality preserved with improved organization
- **Enhanced Reliability**: Comprehensive error handling and monitoring
- **Complete Testing**: 13+ callback registration tests plus existing test suite

## Application Architecture Summary

The application uses a sophisticated modular architecture with:
- **Separation of Concerns**: Each module handles specific functionality
- **Security-First Design**: Comprehensive security measures throughout
- **State Management**: Centralized state handling with pluggable backends
- **Backward Compatibility**: Legacy API support through compatibility layers
- **Comprehensive Testing**: Extensive test coverage including security testing
- **Real-time Updates**: Dash callback system with proper state management
- **Configuration Flexibility**: Modular configuration system with immediate updates across the application
- **Ongoing Refactoring**: Query interface transitioning from monolithic to modular architecture