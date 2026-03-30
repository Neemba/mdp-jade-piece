from dagster import AssetExecutionContext, Output, MetadataValue
from typing import Dict, Any
from pathlib import Path

from mdp_common.dbt import execute_dbt, create_dbt_asset
from ..config import GROUP_SILVER, GROUP_GOLD

#### Silver

dbt_silver_linkage_jade_irium = create_dbt_asset(
    asset_name="silver_linkage_jade_irium",
    dbt_model_name="silver_linkage_jade_irium",
    dbt_project_name="mdp_jade_piece",
    schema_name="JADE_PIECE",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["link_jade_irium"],  # ⚠️ TODO: Valider cette dépendance
    metadata_queries={
        "total_records": "SELECT COUNT(*) FROM JADE_PIECE.B_SILVER_LINKAGE_JADE_IRIUM",
    },
    description="Linkage between Jade and Irium systems",
    dbt_project_dir=Path(__file__).parent / "project",
)

dbt_silver_order_part_bx_jade = create_dbt_asset(
    asset_name="silver_order_part_bx_jade",
    dbt_model_name="silver_order_part_bx",
    dbt_project_name="mdp_jade_piece",
    schema_name="JADE_PIECE",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["order_part_bx_header", "order_part_bx_line"],  # ⚠️ TODO: Valider cette dépendance
    metadata_queries={
        "total_records": "SELECT COUNT(*) FROM JADE_PIECE.B_SILVER_ORDER_PART_BX",
    },
    description="Order part from Jade system",
    dbt_project_dir=Path(__file__).parent / "project",
)

dbt_silver_order_part_mu_jade = create_dbt_asset(
    asset_name="silver_order_part_mu_jade",
    dbt_model_name="silver_order_part_mu",
    dbt_project_name="mdp_jade_piece",
    schema_name="JADE_PIECE",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["order_part_header", "order_part_line"],  # ⚠️ TODO: Valider cette dépendance
    metadata_queries={
        "total_records": "SELECT COUNT(*) FROM JADE_PIECE.B_SILVER_ORDER_PART_MU",
    },
    description="Order part from Jade system",
    dbt_project_dir=Path(__file__).parent / "project",
)

dbt_silver_quote_part_bx_jade = create_dbt_asset(
    asset_name="silver_quote_part_bx_jade",
    dbt_model_name="silver_quote_part_bx",
    dbt_project_name="mdp_jade_piece",
    schema_name="JADE_PIECE",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["quote_part_bx_header", "quote_part_bx_line"],  # ⚠️ TODO: Valider cette dépendance
    metadata_queries={
        "total_records": "SELECT COUNT(*) FROM JADE_PIECE.B_SILVER_QUOTE_PART_BX",
    },
    description="Quote part from Jade system",
    dbt_project_dir=Path(__file__).parent / "project",
)

dbt_silver_quote_part_mu_jade = create_dbt_asset(
    asset_name="silver_quote_part_mu_jade",
    dbt_model_name="silver_quote_part_mu",
    dbt_project_name="mdp_jade_piece",
    schema_name="JADE_PIECE",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["quote_part_header", "quote_part_line"],  # ⚠️ TODO: Valider cette dépendance
    metadata_queries={
        "total_records": "SELECT COUNT(*) FROM JADE_PIECE.B_SILVER_QUOTE_PART_MU",
    },
    description="Quote part from Jade system",
    dbt_project_dir=Path(__file__).parent / "project",
)

dbt_silver_delivery_part_bx_jade = create_dbt_asset(
    asset_name="silver_delivery_part_bx_jade",
    dbt_model_name="silver_delivery_part_bx",
    dbt_project_name="mdp_jade_piece",
    schema_name="JADE_PIECE",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["delivery_part_bx_header", "delivery_part_bx_line"],  # ⚠️ TODO: Valider cette dépendance
    metadata_queries={
        "total_records": "SELECT COUNT(*) FROM JADE_PIECE.B_SILVER_DELIVERY_PART_BX",
    },
    description="Delivery part from Jade system",
    dbt_project_dir=Path(__file__).parent / "project",
)

dbt_silver_delivery_part_mu_jade = create_dbt_asset(
    asset_name="silver_delivery_part_mu_jade",
    dbt_model_name="silver_delivery_part_mu",
    dbt_project_name="mdp_jade_piece",
    schema_name="JADE_PIECE",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["delivery_part_header", "delivery_part_line"],  # ⚠️ TODO: Valider cette dépendance
    metadata_queries={
        "total_records": "SELECT COUNT(*) FROM JADE_PIECE.B_SILVER_DELIVERY_PART_MU",
    },
    description="Delivery part from Jade system",
    dbt_project_dir=Path(__file__).parent / "project",
)
### Gold


