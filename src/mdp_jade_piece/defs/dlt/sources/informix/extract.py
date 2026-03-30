from typing import Dict, Optional
from mdp_common.dlt.extract import extract_with_arrow_informix
from mdp_common.informix import get_informix_connection
from ...configs import DATE_COLUMN_MAPPING, get_table_info
from dagster import AssetExecutionContext
import pyarrow as pa


def get_table_config(table_name: str) -> Dict[str, int]:
    """Get optimized configuration based on table size"""
    table_info = get_table_info(table_name)
    return table_info["batch_config"]


def extract_with_arrow(
    context: AssetExecutionContext,
    table_name: str,
    date_filter: Optional[str],
    query: str,
    get_count: bool = True,
) -> pa.Table:
    """Extract data using Arrow for maximum performance"""
    return extract_with_arrow_informix(
        context=context,
        table_name=table_name,
        date_filter=date_filter,
        query=query,
        get_count=get_count,
        connection_func=get_informix_connection,
        date_column_mapping=DATE_COLUMN_MAPPING,
        get_table_config_func=get_table_config,
        database_schema="ie_delmas:informix"
    )
