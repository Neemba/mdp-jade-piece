"""Basic smoke tests for extraction functionality"""
import pytest
from unittest.mock import Mock, patch
from mdp_company.defs.dlt.sources.informix.extract import get_table_config, extract_with_arrow


def test_get_table_config():
    """Test table configuration retrieval"""
    config = get_table_config("agr_soc")
    assert "fetch_size" in config
    assert "chunk_size" in config
    assert isinstance(config["fetch_size"], int)
    assert isinstance(config["chunk_size"], int)


@patch('mdp_company.defs.dlt.sources.informix.extract.extract_with_arrow_informix')
def test_extract_with_arrow_integration(mock_extract):
    """Test that extract_with_arrow integrates correctly with mdp_common"""
    # Setup mocks
    mock_context = Mock()
    mock_arrow_table = Mock()
    mock_extract.return_value = mock_arrow_table

    # Call the function
    result = extract_with_arrow(
        context=mock_context,
        table_name="agr_soc",
        date_filter=None,
        query="SELECT * FROM test",
        get_count=True
    )

    # Verify mdp_common function was called
    mock_extract.assert_called_once()
    call_args = mock_extract.call_args

    # Verify correct parameters were passed
    assert call_args[1]["table_name"] == "agr_soc"
    assert call_args[1]["query"] == "SELECT * FROM test"
    assert call_args[1]["database_schema"] == "ie_delmas:informix"
    assert callable(call_args[1]["get_table_config_func"])

    # Verify result is returned
    assert result == mock_arrow_table


@patch('mdp_common.dlt.extract.get_incremental_date_filter')
def test_date_filter_import(mock_date_filter):
    """Test that date filter function is correctly imported from mdp_common"""
    from mdp_common.dlt.extract import get_incremental_date_filter

    # Call the imported function
    get_incremental_date_filter("test_table", {}, "incremental", 7)

    # Verify it was called
    mock_date_filter.assert_called_once()


def test_imports_work():
    """Test that all necessary imports work without errors"""
    from mdp_company.defs.dlt.sources.informix.extract import extract_with_arrow, get_table_config
    from mdp_company.defs.dlt.sources.informix.dlt_source import get_agr_soc_data, get_agr_succ_data, get_agr_usr_data

    assert callable(extract_with_arrow)
    assert callable(get_table_config)
    assert callable(get_agr_soc_data)
    assert callable(get_agr_succ_data)
    assert callable(get_agr_usr_data)