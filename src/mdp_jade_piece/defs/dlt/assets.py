"""
Dagster asset definitions for Jade Pieces DB2/AS400 ingestion.

Uses factory functions from mdp_common to create:
- DLT/JDBC assets (via create_db2_dlt_asset)
- ODBC direct load assets (via create_db2_direct_asset)
"""
from mdp_common.dagster.db2_assets import create_db2_dlt_asset, create_db2_direct_asset

from .configs import (
    DATASET_NAME,
    TABLE_CLASSIFICATIONS,
    DATE_COLUMN_MAPPING,
    PRIMARY_KEY_CONFIG,
)
from .sources.as400.dlt_source import (
    db2_cdeent_source,
    db2_cdeent_bx_source,
    db2_devent_source,
    db2_devent_bx_source,
    db2_facent_bx_source,
    db2_faclig_bx_source,
    db2_tiers_source,
    db2_jadirium_source,
    db2_livent_source,
    db2_livent_bx_source,
)
from ..config import GROUP_BRONZE

# ─── DLT / JDBC assets ────────────────────────────────────────────

order_part_header = create_db2_dlt_asset(
    table_name="CDEENT",
    asset_name="order_part_header",
    source_function=db2_cdeent_source,
    pipeline_name="order_part_header",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

order_part_bx_header = create_db2_dlt_asset(
    table_name="CDEENT",
    asset_name="order_part_bx_header",
    source_function=db2_cdeent_bx_source,
    pipeline_name="order_part_bx_header",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

quote_part_header = create_db2_dlt_asset(
    table_name="DEVENT",
    asset_name="quote_part_header",
    source_function=db2_devent_source,
    pipeline_name="quote_part_header",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

quote_part_bx_header = create_db2_dlt_asset(
    table_name="DEVENT",
    asset_name="quote_part_bx_header",
    source_function=db2_devent_bx_source,
    pipeline_name="quote_part_bx_header",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

supply_bx_header = create_db2_dlt_asset(
    table_name="FACENT",
    asset_name="supply_bx_header",
    source_function=db2_facent_bx_source,
    pipeline_name="supply_bx_header",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

third_party = create_db2_dlt_asset(
    table_name="TIERS",
    asset_name="third_party",
    source_function=db2_tiers_source,
    pipeline_name="third_party",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

link_jade_irium = create_db2_dlt_asset(
    table_name="JADIRIUM",
    asset_name="link_jade_irium",
    source_function=db2_jadirium_source,
    pipeline_name="link_jade_irium",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

supply_bx_line = create_db2_dlt_asset(
    table_name="FACLIG",
    asset_name="supply_bx_line",
    source_function=db2_faclig_bx_source,
    pipeline_name="supply_bx_line",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

delivery_part_header = create_db2_dlt_asset(
    table_name="LIVENT",
    asset_name="delivery_part_header",
    source_function=db2_livent_source,
    pipeline_name="delivery_part_header",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

delivery_part_bx_header = create_db2_dlt_asset(
    table_name="LIVENT",
    asset_name="delivery_part_bx_header",
    source_function=db2_livent_bx_source,
    pipeline_name="delivery_part_bx_header",
    dataset_name=DATASET_NAME,
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    group_name=GROUP_BRONZE,
)

# ─── ODBC direct load assets ──────────────────────────────────────

order_part_line = create_db2_direct_asset(
    table_name="CDELIG",
    asset_name="order_part_line",
    dataset_name=DATASET_NAME,
    library="NEGOCE_FIC",
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    primary_key_config=PRIMARY_KEY_CONFIG,
    table_target_key="CDELIG",
    group_name=GROUP_BRONZE,
)

order_part_bx_line = create_db2_direct_asset(
    table_name="CDELIG",
    asset_name="order_part_bx_line",
    dataset_name=DATASET_NAME,
    library="BXNEGO_FIC",
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    primary_key_config=PRIMARY_KEY_CONFIG,
    table_target_key="CDELIGBX",
    group_name=GROUP_BRONZE,
)

quote_part_line = create_db2_direct_asset(
    table_name="DEVLIG",
    asset_name="quote_part_line",
    dataset_name=DATASET_NAME,
    library="NEGOCE_FIC",
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    primary_key_config=PRIMARY_KEY_CONFIG,
    table_target_key="DEVLIG",
    group_name=GROUP_BRONZE,
)

quote_part_bx_line = create_db2_direct_asset(
    table_name="DEVLIG",
    asset_name="quote_part_bx_line",
    dataset_name=DATASET_NAME,
    library="BXNEGO_FIC",
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    primary_key_config=PRIMARY_KEY_CONFIG,
    table_target_key="DEVLIGBX",
    group_name=GROUP_BRONZE,
)

delivery_part_line = create_db2_direct_asset(
    table_name="LIVLIG",
    asset_name="delivery_part_line",
    dataset_name=DATASET_NAME,
    library="NEGOCE_FIC",
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    primary_key_config=PRIMARY_KEY_CONFIG,
    table_target_key="LIVLIG",
    group_name=GROUP_BRONZE,
)

delivery_part_bx_line = create_db2_direct_asset(
    table_name="LIVLIG",
    asset_name="delivery_part_bx_line",
    dataset_name=DATASET_NAME,
    library="BXNEGO_FIC",
    table_classifications=TABLE_CLASSIFICATIONS,
    date_column_mapping=DATE_COLUMN_MAPPING,
    primary_key_config=PRIMARY_KEY_CONFIG,
    table_target_key="LIVLIGBX",
    group_name=GROUP_BRONZE,
)
