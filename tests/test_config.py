import os
import sys
from pathlib import Path

import pytest
import toml

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import Config
from config_manager import get_config, refresh_config

# Store original hardcoded defaults from the Config class
ORIGINAL_CONFIG_DEFAULTS = {
    "DATA_DIR": Config.DATA_DIR,
    "DEMOGRAPHICS_FILE": Config.DEMOGRAPHICS_FILE,
    "PRIMARY_ID_COLUMN": Config.PRIMARY_ID_COLUMN,
    "SESSION_COLUMN": Config.SESSION_COLUMN,
    "COMPOSITE_ID_COLUMN": Config.COMPOSITE_ID_COLUMN,
    "DEFAULT_AGE_SELECTION": Config.DEFAULT_AGE_SELECTION,
    "CONFIG_FILE_PATH": Config.CONFIG_FILE_PATH,
    "_merge_strategy": None, # Reset internal state too
    "_merge_keys": None
}

@pytest.fixture
def mock_streamlit_fixture(monkeypatch): # Renamed to avoid conflict if a 'mock_streamlit' name is used elsewhere
    """Mocks streamlit functions that might be called during config load/save."""
    class MockStreamlit:
        def error(self, msg):
            # print(f"Mocked st.error: {msg}") # Or log, or store for assertions
            pass
        def success(self, msg):
            # print(f"Mocked st.success: {msg}")
            pass
        def warning(self, msg):
            # print(f"Mocked st.warning: {msg}")
            pass
        def info(self, msg):
            # print(f"Mocked st.info: {msg}")
            pass

        class MockCacheData:
            def __init__(self, func):
                self._func = func
            def __call__(self, *args, **kwargs):
                return self._func(*args, **kwargs)
            def clear(self):
                # print(f"Mocked {self._func.__name__}.clear() called")
                pass

        def cache_data(self, *args, **kwargs):
            def decorator(func):
                return self.MockCacheData(func)
            return decorator

    mock_st_instance = MockStreamlit()
    monkeypatch.setattr("main.st", mock_st_instance, raising=False)
    # If get_table_info is directly imported in main and decorated there:
    if hasattr(main, 'get_table_info') and hasattr(main.get_table_info, 'clear'):
         monkeypatch.setattr(main.get_table_info, "clear", lambda: print("Mocked get_table_info.clear() called from main"), raising=False)


@pytest.fixture(autouse=True)
def manage_config_state(tmp_path, monkeypatch, mock_streamlit_fixture): # Uses renamed fixture
    """
    Manages the Config class state for each test:
    1. Resets Config attributes to their original hardcoded defaults.
    2. Temporarily sets CONFIG_FILE_PATH to a path within tmp_path.
    3. Cleans up the temporary config file if created.
    """

    for key, value in ORIGINAL_CONFIG_DEFAULTS.items():
        if key == "CONFIG_FILE_PATH": # This will be monkeypatched specifically
            continue
        monkeypatch.setattr(Config, key, value, raising=False) # raising=False for _merge_strategy/_keys

    test_config_file = tmp_path / "test_config.toml"
    monkeypatch.setattr(Config, "CONFIG_FILE_PATH", str(test_config_file))

    Config.refresh_merge_detection() # Call after defaults are set

    yield str(test_config_file)

    # Monkeypatch handles restoration of attributes it changed.
    # Explicitly restore CONFIG_FILE_PATH to its original dynamic value if necessary,
    # though for class attributes, monkeypatch should handle it.
    # setattr(Config, "CONFIG_FILE_PATH", original_config_file_path_val)

    if test_config_file.exists():
        try:
            test_config_file.unlink()
        except FileNotFoundError:
            pass


# --- Test Cases for Config.load_config() ---

def test_load_config_exists_and_valid(manage_config_state):
    test_config_path = Path(manage_config_state)
    custom_values = {
        "data_dir": "custom_data_dir",
        "demographics_file": "custom_demo.csv",
        "primary_id_column": "custom_pid",
        "session_column": "custom_session",
        "composite_id_column": "custom_cid",
        "default_age_min": 25,
        "default_age_max": 75,
        "sex_mapping": {"F": 1.0, "M": 2.0, "O": 3.0}
    }
    with open(test_config_path, "w") as f:
        toml.dump(custom_values, f)

    Config.load_config()

    assert Config.DATA_DIR == custom_values["data_dir"]
    assert Config.DEMOGRAPHICS_FILE == custom_values["demographics_file"]
    assert Config.PRIMARY_ID_COLUMN == custom_values["primary_id_column"]
    assert Config.SESSION_COLUMN == custom_values["session_column"]
    assert Config.COMPOSITE_ID_COLUMN == custom_values["composite_id_column"]
    assert Config.DEFAULT_AGE_SELECTION == (custom_values["default_age_min"], custom_values["default_age_max"])
    assert Config.SEX_MAPPING == custom_values["sex_mapping"]

def test_load_config_not_exists(manage_config_state):
    test_config_path = Path(manage_config_state)
    assert not test_config_path.exists()

    original_data_dir = ORIGINAL_CONFIG_DEFAULTS["DATA_DIR"]
    original_age_selection = ORIGINAL_CONFIG_DEFAULTS["DEFAULT_AGE_SELECTION"]
    original_sex_mapping = ORIGINAL_CONFIG_DEFAULTS["SEX_MAPPING"]


    Config.load_config()

    assert test_config_path.exists()
    assert Config.DATA_DIR == original_data_dir
    assert Config.DEFAULT_AGE_SELECTION == original_age_selection
    assert Config.SEX_MAPPING == original_sex_mapping


    with open(test_config_path) as f:
        content = toml.load(f)
    assert content["data_dir"] == original_data_dir
    assert content["default_age_min"] == original_age_selection[0]
    assert content["default_age_max"] == original_age_selection[1]
    assert content["sex_mapping"] == original_sex_mapping


def test_load_config_invalid_toml(manage_config_state):
    test_config_path = Path(manage_config_state)
    with open(test_config_path, "w") as f:
        f.write("this is not valid toml content {")

    original_data_dir = ORIGINAL_CONFIG_DEFAULTS["DATA_DIR"]
    original_age_selection = ORIGINAL_CONFIG_DEFAULTS["DEFAULT_AGE_SELECTION"]

    Config.load_config()

    assert Config.DATA_DIR == original_data_dir
    assert Config.DEFAULT_AGE_SELECTION == original_age_selection
    assert test_config_path.exists()

def test_load_config_handles_partial_toml(manage_config_state):
    test_config_path = Path(manage_config_state)
    partial_values = {
        "data_dir": "partial_data_dir",
        "default_age_min": 33
    }
    with open(test_config_path, "w") as f:
        toml.dump(partial_values, f)

    Config.load_config()

    assert Config.DATA_DIR == partial_values["data_dir"]
    assert Config.DEFAULT_AGE_SELECTION[0] == partial_values["default_age_min"]

    assert Config.DEMOGRAPHICS_FILE == ORIGINAL_CONFIG_DEFAULTS["DEMOGRAPHICS_FILE"]
    assert Config.PRIMARY_ID_COLUMN == ORIGINAL_CONFIG_DEFAULTS["PRIMARY_ID_COLUMN"]
    assert Config.DEFAULT_AGE_SELECTION[1] == ORIGINAL_CONFIG_DEFAULTS["DEFAULT_AGE_SELECTION"][1]
    assert Config.SEX_MAPPING == ORIGINAL_CONFIG_DEFAULTS["SEX_MAPPING"]

def test_load_config_calls_refresh_merge_detection(manage_config_state, monkeypatch):
    test_config_path = Path(manage_config_state)
    with open(test_config_path, "w") as f:
        toml.dump({}, f)

    mock_refresh_called_count = 0
    def mock_refresh():
        nonlocal mock_refresh_called_count
        mock_refresh_called_count +=1
        # original_refresh_method() # Avoid calling original if it has side effects not wanted in mock

    monkeypatch.setattr(Config, "refresh_merge_detection", mock_refresh)

    Config.load_config()

    assert mock_refresh_called_count > 0, "Config.load_config() should call refresh_merge_detection()"


# --- Test Cases for Config.save_config() ---

def test_save_config_writes_current_attributes(manage_config_state, monkeypatch):
    test_config_path = Path(manage_config_state)

    modified_data_dir = "saved_data_dir"
    modified_age_selection = (30, 60)
    modified_sex_mapping = {"TestF": 10.0, "TestM": 20.0}

    monkeypatch.setattr(Config, "DATA_DIR", modified_data_dir)
    monkeypatch.setattr(Config, "DEFAULT_AGE_SELECTION", modified_age_selection)
    monkeypatch.setattr(Config, "SEX_MAPPING", modified_sex_mapping)

    Config.save_config()

    assert test_config_path.exists()
    with open(test_config_path) as f:
        saved_content = toml.load(f)

    assert saved_content["data_dir"] == modified_data_dir
    assert saved_content["default_age_min"] == modified_age_selection[0]
    assert saved_content["default_age_max"] == modified_age_selection[1]
    assert saved_content["sex_mapping"] == modified_sex_mapping
    assert saved_content["primary_id_column"] == ORIGINAL_CONFIG_DEFAULTS["PRIMARY_ID_COLUMN"]

def test_save_config_uses_current_attributes_after_load(manage_config_state, monkeypatch):
    test_config_path = Path(manage_config_state)

    initial_toml_values = { "data_dir": "initial_dir", "default_age_min": 20 }
    with open(test_config_path, "w") as f:
        toml.dump(initial_toml_values, f)

    Config.load_config()
    assert Config.DATA_DIR == "initial_dir"
    assert Config.DEFAULT_AGE_SELECTION[0] == 20

    programmatic_data_dir = "programmatic_dir"
    programmatic_age_min = 40

    monkeypatch.setattr(Config, "DATA_DIR", programmatic_data_dir)
    current_age_max = Config.DEFAULT_AGE_SELECTION[1]
    monkeypatch.setattr(Config, "DEFAULT_AGE_SELECTION", (programmatic_age_min, current_age_max))

    Config.save_config()

    with open(test_config_path) as f:
        saved_content = toml.load(f)

    assert saved_content["data_dir"] == programmatic_data_dir
    assert saved_content["default_age_min"] == programmatic_age_min
    assert saved_content["demographics_file"] == ORIGINAL_CONFIG_DEFAULTS["DEMOGRAPHICS_FILE"]
    # Max age should be from the value present after load_config, which was default, or from TOML if specified
    # In this test, initial_toml_values didn't set default_age_max, so it would be the original default's max.
    assert saved_content["default_age_max"] == ORIGINAL_CONFIG_DEFAULTS["DEFAULT_AGE_SELECTION"][1]


# --- Test Case for CLI Override ---

def test_cli_overrides_toml_config(manage_config_state, monkeypatch):
    test_config_path = Path(manage_config_state)

    toml_data_dir = "toml_data_dir_value_cli"
    toml_primary_id = "toml_pid_cli"
    toml_age_max = 77
    toml_values = {
        "data_dir": toml_data_dir,
        "demographics_file": "toml_demo_cli.csv",
        "primary_id_column": toml_primary_id,
        "default_age_min": 22,
        "default_age_max": toml_age_max
    }
    with open(test_config_path, "w") as f:
        toml.dump(toml_values, f)

    Config.load_config()
    assert Config.DATA_DIR == toml_data_dir
    assert Config.PRIMARY_ID_COLUMN == toml_primary_id
    assert Config.DEFAULT_AGE_SELECTION == (22, toml_age_max)

    cli_data_dir = "cli_data_dir_override"
    cli_primary_id = "cli_pid_override"
    cli_age_min = 40

    simulated_argv = [
        "main.py",
        "--",
        "--data-dir", cli_data_dir,
        "--primary-id-column", cli_primary_id,
        "--default-age-min", str(cli_age_min)
    ]
    monkeypatch.setattr(sys, "argv", simulated_argv)

    Config.parse_cli_args()

    assert Config.DATA_DIR == cli_data_dir
    assert Config.PRIMARY_ID_COLUMN == cli_primary_id
    assert Config.DEFAULT_AGE_SELECTION == (cli_age_min, toml_age_max)
    assert Config.DEMOGRAPHICS_FILE == toml_values["demographics_file"]
    assert Config.SESSION_COLUMN == ORIGINAL_CONFIG_DEFAULTS["SESSION_COLUMN"]

# Final check for fixture behavior (optional, more for debugging the fixture)
def test_config_file_path_is_managed(manage_config_state):
    assert Config.CONFIG_FILE_PATH == manage_config_state
    assert Path(manage_config_state).parent.exists() # tmp_path should exist
    assert Path(Config.CONFIG_FILE_PATH).name == "test_config.toml"
    ORIGINAL_CONFIG_DEFAULTS["CONFIG_FILE_PATH"]
    # This assertion can't be made here directly as monkeypatch cleanup happens after test yield.
    # It's an implicit guarantee of monkeypatch.
    # print(f"Original path would be: {original_path_after_test}")
    pass
