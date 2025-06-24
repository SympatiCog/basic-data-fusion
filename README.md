# The Basic Scientist's Basic Data Tool (Plotly Dash Version)

A Plotly Dash-based web application for laboratory research data filtering, querying, merging, and comprehensive data profiling. This tool allows researchers to interactively query, merge, download queried datasets, and generate detailed profiling reports for CSV datasets using an intuitive multipage interface, backed by DuckDB for efficient data processing.

## Who Wants/Needs This Application?

* You have research data stored in multiple CSV files.
* You'd like to be able to:
    * Select a subset of variables from across your CSVs.
    * Merge those variables into a single wide-format CSV for further analysis.
    * Filter out participants based on various criteria.
* **You prefer a GUI over manual scripting for these tasks and want to avoid error-prone spreadsheet operations.**
* You want the power and efficiency of SQL without needing to set up or manage a traditional SQL database/server.

**Key advantages:**
- **Local Operation**: Runs on your local machine, no IT department dependency for core use.
- **Easy Data Updates**: Update your data by modifying your CSV files in the designated data folder.
- **No Database Administration**: Leverages DuckDB for on-the-fly SQL processing of CSVs.
- **Familiar Workflow**: Provides an interactive experience for data manipulation and analysis.

## Features

### 🚀 **Guided Onboarding Experience**
- **Stepwise Setup Process**: New users are guided through a structured, wizard-like configuration process.
- **Progressive Widget Enablement**: Interface elements unlock as previous steps are completed.
- **Smart Column Detection**: Automatically populates dropdown options with actual column names from uploaded demographics files.
- **Real-time Validation**: Immediate feedback on file uploads and configuration choices.
- **Load Existing Configurations**: Support for importing TOML/JSON configuration files.
- **Seamless Transition**: Automatic redirect to main application after successful setup.

### 🔍 **Data Query & Merging**
- **Smart Data Structure Detection**: Automatically detects cross-sectional vs. longitudinal data formats.
- **Flexible Column Configuration**: Adapts to common column naming conventions for participant IDs and session identifiers.
- **Interactive Data Filtering**: Apply demographic (age, study-specific) and phenotypic filters (numeric ranges and categorical selections from any table).
- **Real-time Participant Count**: See matching participant counts update as you adjust filters.
- **Intelligent Table Merging**: Merges data based on detected or configured merge keys.
- **Flexible Column Selection**: Choose specific columns from each table for export.
- **Data Pivoting (Enwiden)**: Transform longitudinal data from long to wide format (e.g., `age_BAS1`, `age_BAS2`).
- **Fast Performance**: Utilizes DuckDB for efficient query execution on CSVs.
- **Export Functionality**: Download filtered and merged datasets as CSV files.

### 📋 **Filtering Summary Reports**
- **Comprehensive Filter Tracking**: Generate detailed reports documenting the complete filtering process and its impact on sample size.
- **Two-Report System**: 
  - **Filtering Steps Report**: Sequential documentation of each filter applied (age, substudy/site, session, phenotypic) with participant counts and demographic breakdowns at each step.
  - **Final Dataset Summary**: Descriptive statistics for all variables in the final filtered dataset, including mean, median, standard deviation, min/max values, and missing data counts.
- **Multisite & Multisession Support**: Enhanced tracking for datasets with multiple study sites/substudies and longitudinal sessions/visits.
- **Smart Filtering Sequence**: Automatically applies filters in optimal order (substudy → session → age → phenotypic) and only reports steps where actual filtering occurred.
- **Publication-Ready Output**: Generates clean CSV reports suitable for manuscript methods sections and data auditing.
- **Categorical Data Handling**: For categorical variables, displays value:count pairs (e.g., 'caucasian/white':90, 'hispanic':24).
- **Longitudinal Data Features**: For longitudinal datasets, includes both collapsed summaries and per-session breakdowns.
- **Demographic Tracking**: Monitors age ranges and sex distributions (Male/Female/Other) throughout the filtering process.
- **ZIP Download**: Both reports are bundled together with custom naming options for immediate download.

### 📊 **Data Profiling & Analysis**
- **Comprehensive Data Profiling**: Generate detailed statistical analysis reports using `ydata-profiling`.
- **Interactive Visualizations**: Includes correlation matrices, distribution plots, missing values heatmaps, etc., within the report.
- **Multiple Report Types**: Select Full, Minimal, or Explorative profiling modes.
- **Performance Optimized**: Option to use sampling for large datasets.
- **Export Reports**: Download detailed HTML and JSON profiling reports.
- **Standalone Analysis**: Upload CSV files directly on the profiling page for analysis.
- **Data Quality Assessment**: Identify missing values, outliers, and data type issues through the report.

### 📈 **Interactive Data Plotting & Statistical Analysis**
- **Multiple Plot Types**: Create scatter plots, histograms, box plots, violin plots, and density heatmaps with dynamic configuration.
- **Interactive Statistical Controls**: Dynamic checkbox interface that adapts to plot type for selective analysis display:
  - **Scatter Plot Controls**: Toggle OLS trendlines and statistical summaries independently
  - **Histogram Controls**: Individual toggles for mean lines, median lines, KDE curves, and statistical summaries
  - **Box/Violin Plot Controls**: Toggle group statistics and comprehensive ANOVA analysis
  - **Real-time Updates**: All visual overlays and summaries respond immediately to checkbox changes
  - **Clean State Management**: No overlay accumulation or legend duplication when toggling features
- **OLS Regression Analysis**: Comprehensive linear regression analysis for scatter plots including:
  - Interactive trendline overlay with R-squared display in legend
  - Complete statistical output: slope, intercept, correlation coefficients
  - P-values, standard errors, and significance testing with interpretation
  - Effect size assessment and regression equation display
- **Comprehensive Distribution Analysis**: Advanced histogram statistics and visualization including:
  - Complete descriptive statistics (mean, median, std, variance, skewness, kurtosis)
  - Quartile and percentile analysis with IQR calculations
  - Normality testing (Shapiro-Wilk, Anderson-Darling) with detailed interpretations
  - Interactive visual overlays: color-coded mean/median lines, KDE curve estimation
  - Automatic distribution shape assessment and statistical interpretation
- **Advanced ANOVA & T-Test Analysis**: Professional statistical analysis for box and violin plots including:
  - One-way ANOVA with F-statistics, p-values, and degrees of freedom
  - Complete ANOVA table with sum of squares, mean squares, and effect sizes (eta-squared)
  - Pairwise t-tests between all group combinations with Bonferroni correction
  - Cohen's d effect sizes for practical significance assessment
  - Group descriptive statistics with sample sizes, means, and standard deviations
  - Publication-ready statistical output with significance indicators (*, **, ***)
- **Modular Analysis Architecture**: Extensible callback system supporting complex statistical workflows
- **Cross-Filtering**: Select data points on plots to filter and export corresponding table rows.
- **Interactive Plot Configuration**: Real-time column selection based on data types (numeric/categorical).
- **Advanced Aesthetics**: Configure color, size, and faceting options for enhanced visualization.
- **Data Export**: Export selected data points or full datasets as CSV files.
- **Drag-and-Drop Upload**: Load CSV files directly into the plotting interface.

### 🏗️ **Application Structure**
- **Multipage Interface**: Organized navigation between Setup, Data Query, Import, Profiling, and Settings pages.
- **Empty State Handling**: Automatically redirects new users to the guided onboarding process.
- **Session State Management**: Shares merged data from the Query page to the Profiling page.
- **Responsive Design**: Built with Dash Bootstrap Components for usability on various screen sizes.

### ⚙️ **Configuration & Management**
- **TOML-based Configuration**: Uses `config.toml` for persistent settings like data directory, default column names, and UI preferences.
- **Automatic Creation**: `config.toml` is created with default values on first run if not present.

## Setup Instructions

### Prerequisites

- Python 3.10 or higher
- Pip (Python package installer)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/SympatiCog/labdata_fqm.git
    cd labdata_fqm
    ```

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    # For Unix/macOS
    python3 -m venv .venv
    source .venv/bin/activate

    # For Windows
    python -m venv .venv
    .venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

### First-Time Setup (Guided Onboarding)

1.  **Run the Dash application:**
    ```bash
    python app.py
    ```

2.  **Open your web browser** and navigate to `http://127.0.0.1:8050/`.

3.  **Follow the guided setup process:**
    - **Step 1**: Upload your demographics CSV file
    - **Step 2**: Configure age column using detected column names
    - **Step 3**: Set up CSV linking information (ID columns, sessions)
    - **Step 4**: Upload additional data files via drag & drop
    - **Complete**: Automatic redirect to the main application

### Subsequent Usage

Once configured, simply run `python app.py` and navigate to `http://127.0.0.1:8050/` to access the full application interface directly.

### Alternative Setup Methods

- **Load Existing Configuration**: Use the "Load Configuration File" button to import a previously saved TOML or JSON configuration.
- **Manual Setup**: Access the traditional Settings page for advanced configuration options.

## Configuration (`config.toml`)

The application uses a `config.toml` file for configuration. If this file does not exist when the application starts, it will be created automatically with default settings.

**Key configuration options:**

*   `DATA_DIR`: Path to your CSV data files (e.g., "data", "my_research_data/csvs").
*   `DEMOGRAPHICS_FILE`: Filename of your primary demographics CSV (e.g., "demographics.csv").
*   `PRIMARY_ID_COLUMN`: Default name for the primary subject identifier column (e.g., "ursi", "subject_id").
*   `SESSION_COLUMN`: Default name for the session identifier column for longitudinal data (e.g., "session_num", "visit").
*   `COMPOSITE_ID_COLUMN`: Default name for the column that will store the combined ID+Session for merging longitudinal data (e.g., "customID").
*   `AGE_COLUMN`: Default name for the age column used in demographic filtering (e.g., "age").
*   `SEX_COLUMN`: Default name for the sex/gender column used in demographic breakdowns (e.g., "sex").
*   `STUDY_SITE_COLUMN`: Default name for the study site/substudy column for multisite data tracking (e.g., "study_site").
*   `DEFAULT_AGE_SELECTION`: Default age range selected in the UI (e.g., `[18, 80]`).

You can edit `config.toml` directly to change these settings. The application reads this file on startup.

## Project Structure

*   `app.py`: Main Dash application entry point, defines the overall app layout, navbar, and empty state detection.
*   `pages/`: Directory containing individual page modules for the Dash app.
    *   `onboarding.py`: Guided setup process for new users.
    *   `query.py`: Logic and layout for the Data Query & Merge page.
    *   `import.py`: Data import and file upload functionality.
    *   `profiling.py`: Logic and layout for the Data Profiling page.
    *   `03_📈_Data_Plotting.py`: Interactive data visualization and plotting with OLS regression analysis.
    *   `settings.py`: Advanced configuration management interface.
*   `utils.py`: Utility functions, including data processing, query generation, and configuration management.
*   `config_manager.py`: Centralized configuration singleton management.
*   `assets/`: Directory for CSS or JavaScript files (if any).
*   `data/`: Default directory for user's CSV data files (can be changed in `config.toml`).
*   `config.toml`: Configuration file (auto-generated if not present).
*   `requirements.txt`: Python package dependencies.
*   `README.md`: This file.

## Development

To set up for development, follow the installation instructions above.
The application runs in debug mode by default when using `python app.py`, which enables hot-reloading.

## License

This project is for research and educational use.
