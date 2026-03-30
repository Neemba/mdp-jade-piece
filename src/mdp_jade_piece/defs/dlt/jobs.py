import dagster as dg
from ..config import GROUP_BRONZE


daily_job = dg.define_asset_job(
    name="as400_daily_job",
    description="Daily incremental load",
    selection=dg.AssetSelection.groups(GROUP_BRONZE),
    config={
        "ops": {
            # Tables with incremental support (date-based filtering)
            "order_part_header": {
                "config": {
                    "load_mode": "incremental",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "supply_bx_header": {
                "config": {
                    "load_mode": "incremental",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
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
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "order_part_bx_header": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "order_part_line": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "order_part_bx_line": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "supply_bx_header": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "supply_bx_line": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "third_party": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "delivery_part_header": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "delivery_part_bx_header": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "delivery_part_line": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
            "delivery_part_bx_line": {
                "config": {
                    "load_mode": "full",
                    "extraction_method": "auto",
                    "jdbc_threshold": 10000,
                    "unload_direct_threshold": 500000,
                    "get_count": True,
                    "persist_parquet": True,
                }
            },
        },
    },
)
