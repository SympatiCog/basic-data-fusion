# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Dash-based data browser application for laboratory research data. The app allows researchers to filter, query, merge, and export CSV datasets using an interactive multi-page web interface backed by DuckDB for efficient SQL queries.

## Key Architecture

- **Main Application**: `app.py` - Dash multi-page application with centralized routing
- **Pages**: Multi-page architecture with dedicated functionality:
  - `pages/query.py` - Main data query and filtering interface (path: `/`)
  - `pages/import.py` - Data import and file upload functionality (path: `/import`)
  - `pages/settings.py` - Configuration management interface (path: `/settings`)
  - `pages/profiling.py` / `pages/02_📊_Data_Profiling.py` - Data profiling and exploration (path: `/profiling`)
  - `pages/plotting.py` - Interactive data visualization and plotting (path: `/plotting`)
- **Configuration**: Centralized configuration system using TOML files and dataclasses
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

The project includes comprehensive testing:
- **Test Directory**: `tests/` contains all test files
- **Fixtures**: `tests/fixtures/` provides sample datasets for testing:
  - `cross_sectional/` - Test data for cross-sectional analysis
  - `longitudinal/` - Test data for longitudinal analysis  
  - `rockland/` - Additional test datasets
- **Test Coverage**: 
  - Configuration management (`test_config.py`)
  - Core functionality (`test_core.py`)
  - Data processing (`test_data_processing.py`)
  - File upload (`test_file_upload.py`)
  - Integration tests (`test_integration.py`)
  - SQL generation (`test_sql_generation.py`)
- **Quality Tools**: Configured with pytest, coverage reporting, ruff linting, and mypy type checking

## Configuration Management

The application uses a sophisticated configuration system with the following components:

### Configuration Files
- `config.toml` - Main configuration file with all settings
- `config_manager.py` - Singleton pattern for centralized config management
- `utils.py` - Config dataclass definition and file I/O operations

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

### Data Processing
- `get_table_info()`: Scans data directory, detects structure, returns metadata (cached 10 minutes)
- `MergeKeys`: Encapsulates merge column information and dataset structure
- `FlexibleMergeStrategy`: Auto-detects and handles cross-sectional vs longitudinal data
- `generate_base_query_logic()`: Creates FROM/JOIN/WHERE clauses with flexible merge keys
- `generate_data_query()`: Builds full SELECT query for data export
- `generate_count_query()`: Builds COUNT query for participant matching
- `enwiden_longitudinal_data()`: Pivots longitudinal data from long to wide format

### File Operations
- `validate_csv_file()`: Comprehensive CSV file validation
- `save_uploaded_files_to_data_dir()`: Secure file saving with validation
- `get_db_connection()`: Cached DuckDB connection management

## Application Features

### Query Interface
- Real-time participant count updates
- Dynamic demographic and phenotypic filters
- Session-based filtering for longitudinal data
- Export options (CSV, long/wide format)
- Merge strategy visualization

### Phenotypic Filtering System
- Table and column selection with dynamic options
- Support for numeric (range) and categorical (selection) filters
- Real-time filter validation and participant count updates
- Complex multi-table filtering logic

### State Management
- Dash stores for session persistence
- Real-time updates across components
- Prevention of callback interference
- Smart initialization and refresh patterns

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

The application uses a sophisticated callback system with proper state management, real-time updates, and comprehensive error handling. All configuration changes are immediately reflected across the application through the centralized configuration management system.