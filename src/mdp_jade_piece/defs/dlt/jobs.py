import dagster as dg
from ..config import GROUP_BRONZE


# DB2LoadConfig fields: load_mode, days_back, extraction_method
# The extraction method (jdbc vs odbc_direct) is already determined by
# which factory created each asset, so we only configure load_mode and days_back.

daily_job = dg.define_asset_job(
    name="as400_daily_job",
    description="Daily incremental load",
    selection=dg.AssetSelection.groups(GROUP_BRONZE),
    config={
        "ops": {
            "order_part_header": {
                "config": {
                    "load_mode": "incremental",
                    "days_back": 7,
                }
            },
            "supply_bx_header": {
                "config": {
                    "load_mode": "incremental",
                    "days_back": 7,
                }
            },
        },
    },
)


full_refresh_job = dg.define_asset_job(
    name="as400_full_refresh_job",
    description="Full refresh of all tables",
    selection=dg.AssetSelection.groups(GROUP_BRONZE),
    config={
        "ops": {
            "order_part_header": {
                "config": {
                    "load_mode": "full",
                }
            },
            "order_part_bx_header": {
                "config": {
                    "load_mode": "full",
                }
            },
            "order_part_line": {
                "config": {
                    "load_mode": "full",
                }
            },
            "order_part_bx_line": {
                "config": {
                    "load_mode": "full",
                }
            },
            "supply_bx_header": {
                "config": {
                    "load_mode": "full",
                }
            },
            "supply_bx_line": {
                "config": {
                    "load_mode": "full",
                }
            },
            "third_party": {
                "config": {
                    "load_mode": "full",
                }
            },
            "delivery_part_header": {
                "config": {
                    "load_mode": "full",
                }
            },
            "delivery_part_bx_header": {
                "config": {
                    "load_mode": "full",
                }
            },
            "delivery_part_line": {
                "config": {
                    "load_mode": "full",
                }
            },
            "delivery_part_bx_line": {
                "config": {
                    "load_mode": "full",
                }
            },
        },
    },
)
