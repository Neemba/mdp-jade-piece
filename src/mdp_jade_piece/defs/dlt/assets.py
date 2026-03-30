from dagster import asset, AssetExecutionContext, Output, MetadataValue
from dagster_dlt import DagsterDltResource, DagsterDltTranslator
from dagster import AssetKey
import dlt as dlt_lib
from mdp_common import (
    InformixLoadConfig,
    direct_extract_and_load,
    get_informix_connection,
)
from mdp_common.informix.ssh import get_row_count_ssh
from mdp_common.dlt.extract import get_incremental_date_filter

from .configs import (
    DATASET_NAME,
    TABLE_CLASSIFICATIONS,
    DATE_COLUMN_MAPPING,
    PRIMARY_KEY_CONFIG,
    WRITE_DISPOSITION_MAP,
    FORCE_JDBC_TABLES,
    JDBC_THRESHOLD,
    UNLOAD_DIRECT_THRESHOLD,
    get_database_schema,
    get_optimized_runtime_config,
)
from .sources.as400.dlt_source import (
    db2_cdeent_source,
    db2_cdeent_bx_source,
    db2_cdelig_source,
    db2_facent_bx_source,
    db2_tiers_source,
    db2_cdelig_bx_source,
    db2_faclig_bx_source,
    db2_devent_source,
    db2_devent_bx_source,
    db2_jadirium_source,
    db2_livent_source,
    db2_livent_bx_source,
)
from .sources.as400.unload_direct import extract_and_stage_to_snowflake
from ..config import GROUP_BRONZE


class CustomDltTranslator(DagsterDltTranslator):
    def __init__(self, asset_name: str):
        self.asset_name = asset_name

    def get_asset_key(self, resource) -> AssetKey:
        return AssetKey(self.asset_name) 

def create_db2_asset(
    table_name: str,
    asset_name: str,
    source_function,
    pipeline_name: str,
):
    @asset(
        name=asset_name,
        group_name=GROUP_BRONZE,
        kinds={"python", "snowflake", "dlthub"},
    )
    def db2_asset(
        context: AssetExecutionContext,
        dlt: DagsterDltResource,
        config: InformixLoadConfig,
    ):
        #Initialiser le config (Actu valeur par defaut)
        context.log.info(f"🔀 DB2 ASSET: {asset_name} — table: {table_name}")

        load_type = "full" if config.load_mode == "full" else "incremental"

        source = source_function(
            #load_mode=config.load_mode,
            days_back=config.days_back,
        )
        write_disposition = "replace" #WRITE_DISPOSITION_MAP.get(config.load_mode, "merge") # A décommenter pour une config prédéfinie

        dlt_pipeline = dlt_lib.pipeline(
            pipeline_name=pipeline_name,
            destination="snowflake",
            dataset_name=DATASET_NAME,
            progress="log",
            staging=None,
        )

        yield from dlt.run(
            context=context,
            dlt_source=source,
            dlt_pipeline=dlt_pipeline,
            dagster_dlt_translator=CustomDltTranslator(asset_name=asset_name),
            write_disposition=write_disposition,
            loader_file_format="parquet",
        )

    return db2_asset

def create_as400_asset(
    table_name: str,
    table_target_name : str,
    library : str,
    asset_name: str,
    #pipeline_name: str,
):
    @asset(
        name=asset_name,
        group_name=GROUP_BRONZE,
        kinds={"python", "snowflake"},
    )
    def as400_asset(
        context: AssetExecutionContext,
        config: InformixLoadConfig,
    ) -> Output:
        context.log.info(f"🚀 DB2 DIRECT LOAD: {asset_name}")

        date_column = DATE_COLUMN_MAPPING.get(table_name, {}).get("column")
        target_table = TABLE_CLASSIFICATIONS[table_target_name]["target_name"]

        result = extract_and_stage_to_snowflake(
            context=context,
            table_name=table_name,
            target_table_name=target_table,
            schema_name=DATASET_NAME,
            library=library,
            date_column=date_column,
            days_back=config.days_back,
            load_mode="full",
            chunk_size=100000,
        )

        yield Output(
            value=result,
            metadata={
                "pipeline":    MetadataValue.text("AS400 ODBC → Snowflake COPY INTO"),
                "rows_loaded": MetadataValue.int(result.get("rows_loaded", 0)),
                "chunks":      MetadataValue.int(result.get("chunks", 0)),
                "mode":        MetadataValue.text(result.get("mode", "")),
            },
        )

    return as400_asset
# Instanciation
order_part_header = create_db2_asset(
    table_name="CDEENT",
    asset_name="order_part_header",
    source_function=db2_cdeent_source,
    pipeline_name="order_part_header",
)

order_part_bx_header = create_db2_asset(
    table_name="CDEENT",
    asset_name="order_part_bx_header",
    source_function=db2_cdeent_bx_source,
    pipeline_name="order_part_bx_header",
)

quote_part_header = create_db2_asset(
    table_name="DEVENT",
    asset_name="quote_part_header",
    source_function=db2_devent_source,
    pipeline_name="quote_part_header",
)

quote_part_bx_header = create_db2_asset(
    table_name="DEVENT",
    asset_name="quote_part_bx_header",
    source_function=db2_devent_bx_source,
    pipeline_name="quote_part_bx_header",
)

order_part_line = create_as400_asset(
    table_name="CDELIG",
    table_target_name="CDELIG",
    library = "NEGOCE_FIC",
    asset_name="order_part_line",
)

quote_part_line = create_as400_asset(
    table_name="DEVLIG",
    table_target_name="DEVLIG",
    library = "NEGOCE_FIC",
    asset_name="quote_part_line",
)

quote_part_bx_line = create_as400_asset(
    table_name="DEVLIG",
    table_target_name="DEVLIGBX",
    library = "BXNEGO_FIC",
    asset_name="quote_part_bx_line",
)

supply_bx_header = create_db2_asset(
    table_name="FACENT",
    asset_name="supply_bx_header",
    source_function=db2_facent_bx_source,
    pipeline_name="supply_bx_header",
)

third_party = create_db2_asset(
    table_name="TIERS",
    asset_name="third_party",
    source_function=db2_tiers_source,
    pipeline_name="third_party",
)

link_jade_irium = create_db2_asset(
    table_name="JADIRIUM",
    asset_name="link_jade_irium",
    source_function=db2_jadirium_source,
    pipeline_name="link_jade_irium",
)

order_part_bx_line = create_db2_asset(
    table_name="CDELIG",
    asset_name="order_part_bx_line",
    source_function=db2_cdelig_bx_source,
    pipeline_name="order_part_bx_line",
)

supply_bx_line = create_db2_asset(
    table_name="FACLIG",
    asset_name="supply_bx_line",
    source_function=db2_faclig_bx_source,
    pipeline_name="supply_bx_line",
)

delivery_part_header = create_db2_asset(
    table_name="LIVENT",
    asset_name="delivery_part_header",
    source_function=db2_livent_source,
    pipeline_name="delivery_part_header",
)

delivery_part_bx_header = create_db2_asset(
    table_name="LIVENT",
    asset_name="delivery_part_bx_header",
    source_function=db2_livent_bx_source,
    pipeline_name="delivery_part_bx_header",
)

delivery_part_line = create_as400_asset(
    table_name="LIVLIG",
    table_target_name="LIVLIG",
    library = "NEGOCE_FIC",
    asset_name="delivery_part_line",
)

delivery_part_bx_line = create_as400_asset(
    table_name="LIVLIG",
    table_target_name="LIVLIGBX",
    library = "BXNEGO_FIC",
    asset_name="delivery_part_bx_line",
)
