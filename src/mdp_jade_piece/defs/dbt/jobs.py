from mdp_common.dbt import create_dbt_job


refresh_silver_linkage_jade_irium = create_dbt_job(
    asset_name="silver_linkage_jade_irium",
    job_name="refresh_silver_linkage_jade_irium",
    description="Refresh silver linkage jade irium (triggered by bronze asset updates)",
)

refresh_silver_order_part_bx_jade = create_dbt_job(
    asset_name="silver_order_part_bx_jade",
    job_name="refresh_silver_order_part_bx_jade",
    description="Refresh silver order part bx jade (triggered by bronze asset updates)",
)

refresh_silver_order_part_mu_jade = create_dbt_job(
    asset_name="silver_order_part_mu_jade",
    job_name="refresh_silver_order_part_mu_jade",
    description="Refresh silver order part mu jade (triggered by bronze asset updates)",
)

refresh_silver_quote_part_bx_jade = create_dbt_job(
    asset_name="silver_quote_part_bx_jade",
    job_name="refresh_silver_quote_part_bx_jade",
    description="Refresh silver quote part bx jade (triggered by bronze asset updates)",
)

refresh_silver_quote_part_mu_jade = create_dbt_job(
    asset_name="silver_quote_part_mu_jade",
    job_name="refresh_silver_quote_part_mu_jade",
    description="Refresh silver quote part mu jade (triggered by bronze asset updates)",
)

refresh_silver_delivery_part_bx_jade = create_dbt_job(
    asset_name="silver_delivery_part_bx_jade",
    job_name="refresh_silver_delivery_part_bx_jade",
    description="Refresh silver delivery part bx jade (triggered by bronze asset updates)",
)

refresh_silver_delivery_part_mu_jade = create_dbt_job(
    asset_name="silver_delivery_part_mu_jade",
    job_name="refresh_silver_delivery_part_mu_jade",
    description="Refresh silver delivery part mu jade (triggered by bronze asset updates)",
)
########## GOLD ############




