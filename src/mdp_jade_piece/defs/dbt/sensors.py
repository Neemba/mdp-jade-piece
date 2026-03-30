"""
DBT Asset Sensors.

Auto-trigger dbt assets when their dependencies are ready.
"""

from mdp_common.dbt import create_multi_asset_sensor


# Bronze → Silver work order headers and lines
silver_linkage_jade_irium_sensor = create_multi_asset_sensor(
    sensor_name="silver_linkage_jade_irium_sensor",
    monitored_asset_names=["link_jade_irium"],  # ⚠️ TODO: Valider cet asset à monitorer
    job_name="refresh_silver_linkage_jade_irium",
    minimum_interval_seconds=30,
)

silver_order_part_bx_jade_sensor = create_multi_asset_sensor(
    sensor_name="silver_order_part_bx_jade_sensor",
    monitored_asset_names=["order_part_bx_header", "order_part_bx_line"],  # ⚠️ TODO: Valider cet asset à monitorer
    job_name="refresh_silver_order_part_bx_jade",
    minimum_interval_seconds=30,
)

silver_order_part_mu_jade_sensor = create_multi_asset_sensor(
    sensor_name="silver_order_part_mu_jade_sensor",
    monitored_asset_names=["order_part_header", "order_part_line"],  # ⚠️ TODO: Valider cet asset à monitorer
    job_name="refresh_silver_order_part_mu_jade",
    minimum_interval_seconds=30,
)

silver_quote_part_bx_jade_sensor = create_multi_asset_sensor(
    sensor_name="silver_quote_part_bx_jade_sensor",
    monitored_asset_names=["quote_part_bx_header", "quote_part_bx_line"],  # ⚠️ TODO: Valider cet asset à monitorer
    job_name="refresh_silver_quote_part_bx_jade",
    minimum_interval_seconds=30,
)

silver_quote_part_mu_jade_sensor = create_multi_asset_sensor(
    sensor_name="silver_quote_part_mu_jade_sensor",
    monitored_asset_names=["quote_part_header", "quote_part_line"],  # ⚠️ TODO: Valider cet asset à monitorer
    job_name="refresh_silver_quote_part_mu_jade",
    minimum_interval_seconds=30,
)

silver_delivery_part_bx_jade_sensor = create_multi_asset_sensor(
    sensor_name="silver_delivery_part_bx_jade_sensor",
    monitored_asset_names=["delivery_part_bx_header", "delivery_part_bx_line"],  # ⚠️ TODO: Valider cet asset à monitorer
    job_name="refresh_silver_delivery_part_bx_jade",
    minimum_interval_seconds=30,
)

silver_delivery_part_mu_jade_sensor = create_multi_asset_sensor(
    sensor_name="silver_delivery_part_mu_jade_sensor",
    monitored_asset_names=["delivery_part_header", "delivery_part_line"],  # ⚠️ TODO: Valider cet asset à monitorer
    job_name="refresh_silver_delivery_part_mu_jade",
    minimum_interval_seconds=30,
)
######### GOLD ##########



