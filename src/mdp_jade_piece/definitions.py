from dagster import definitions, Definitions
from mdp_common.dagster.resources import (
    create_dlt_resource,
    create_snowflake_resource,
)

### dlt
from .defs.dlt.configs import DATASET_NAME

from .defs.dlt.assets import (
    order_part_header,
    order_part_bx_header,
    order_part_line,
    supply_bx_header,
    third_party,
    order_part_bx_line,
    supply_bx_line,
    quote_part_bx_header,
    quote_part_header,
    quote_part_bx_line,
    quote_part_line,
    link_jade_irium,
    delivery_part_bx_header,
    delivery_part_header,
    delivery_part_bx_line,
    delivery_part_line,
)

from .defs.dlt.jobs import (
    daily_job,
    full_refresh_job,
)

from .defs.dlt.schedules import (
    daily_schedule,
    weekly_full_refresh,
)

### dbt

#from mdp_jade_piece.defs.dbt.assets import (
from .defs.dbt.assets import (
    dbt_silver_linkage_jade_irium,
    dbt_silver_order_part_bx_jade,
    dbt_silver_order_part_mu_jade,
    dbt_silver_delivery_part_bx_jade,
    dbt_silver_delivery_part_mu_jade,
    dbt_silver_quote_part_bx_jade,
    dbt_silver_quote_part_mu_jade,
)
from .defs.dbt.jobs import (
    refresh_silver_linkage_jade_irium,
    refresh_silver_order_part_bx_jade,
    refresh_silver_order_part_mu_jade,
    refresh_silver_quote_part_bx_jade,
    refresh_silver_quote_part_mu_jade,
    refresh_silver_delivery_part_bx_jade,
    refresh_silver_delivery_part_mu_jade,
)
from .defs.dbt.sensors import (
    silver_linkage_jade_irium_sensor,
    silver_order_part_bx_jade_sensor,
    silver_order_part_mu_jade_sensor,
    silver_quote_part_bx_jade_sensor,
    silver_quote_part_mu_jade_sensor,
    silver_delivery_part_bx_jade_sensor,
    silver_delivery_part_mu_jade_sensor,
)
###from .defs.dbt.checks import (
###    check_exchange_rates_freshness,
###    check_all_currencies_have_rates,
###    check_positive_exchange_rates,
###    check_currency_codes_not_empty,
###)


@definitions
def defs():
    """Main definitions combining base resources with company assets and jobs."""

    return Definitions(
        assets=[
            # Bronze/DLT assets (from AS400)
            order_part_header,
            order_part_bx_header,
            order_part_line,
            supply_bx_header,
            supply_bx_line,
            third_party,
            order_part_bx_line,
            quote_part_bx_header,
            quote_part_header,
            quote_part_bx_line,
            quote_part_line,
            link_jade_irium,
            delivery_part_bx_header,
            delivery_part_header,
            delivery_part_bx_line,
            delivery_part_line,


            # Bronze/Static assets (from CSV files)
            # Silver/DBT assets
            dbt_silver_linkage_jade_irium,
            dbt_silver_order_part_bx_jade,
            dbt_silver_order_part_mu_jade,
            dbt_silver_delivery_part_bx_jade,
            dbt_silver_delivery_part_mu_jade,
            dbt_silver_quote_part_bx_jade,
            dbt_silver_quote_part_mu_jade,

            # Gold/DBT assets
            # TODO: Dbt Gold  

            # External assets from mdp-params
        ],
        resources={
            "dlt": create_dlt_resource(),
            "snowflake": create_snowflake_resource(DATASET_NAME),
        },
        jobs=[
            # DLT jobs
            daily_job,
            full_refresh_job,

            # DBT jobs - Silver
            refresh_silver_linkage_jade_irium,
            refresh_silver_order_part_bx_jade,
            refresh_silver_order_part_mu_jade,
            refresh_silver_quote_part_bx_jade,
            refresh_silver_quote_part_mu_jade,
            refresh_silver_delivery_part_bx_jade,
            refresh_silver_delivery_part_mu_jade,
            

            # DBT jobs - Gold
            # TODO: Dbt Gold
            # Static CSV jobs
        ],
        schedules=[
            daily_schedule,
            weekly_full_refresh,
        ],
        sensors=[
            # TODO: Sensor Silver + Gold
            silver_linkage_jade_irium_sensor,
            silver_order_part_bx_jade_sensor,
            silver_order_part_mu_jade_sensor,
            silver_quote_part_bx_jade_sensor,
            silver_quote_part_mu_jade_sensor,
            silver_delivery_part_bx_jade_sensor,            
            silver_delivery_part_mu_jade_sensor,
        ],
    )
