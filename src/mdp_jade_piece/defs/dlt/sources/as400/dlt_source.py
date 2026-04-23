"""
DB2/AS400 DLT sources for Jade Pieces.

Each source is created via create_db2_dlt_source() from mdp_common,
replacing the previous 12 copy-pasted @dlt.source functions.
"""
from mdp_common.dlt.db2_source import create_db2_dlt_source
from ...configs import (
    TABLE_CLASSIFICATIONS,
    PRIMARY_KEY_CONFIG,
    get_column_hints,
)

# ─── NEGOCE_FIC sources ────────────────────────────────────────────

db2_cdeent_source = create_db2_dlt_source(
    table_name="CDEENT",
    target_name=TABLE_CLASSIFICATIONS["CDEENT"]["target_name"],
    schema="NEGOCE_FIC",
    write_disposition="replace",
    primary_keys=PRIMARY_KEY_CONFIG["CDEENT"]["keys"],
    column_hints=get_column_hints("CDEENT"),
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_devent_source = create_db2_dlt_source(
    table_name="DEVENT",
    target_name=TABLE_CLASSIFICATIONS["DEVENT"]["target_name"],
    schema="NEGOCE_FIC",
    write_disposition="replace",
    primary_keys=PRIMARY_KEY_CONFIG["DEVENT"]["keys"],
    column_hints=get_column_hints("DEVENT"),
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_livent_source = create_db2_dlt_source(
    table_name="LIVENT",
    target_name=TABLE_CLASSIFICATIONS["LIVENT"]["target_name"],
    schema="NEGOCE_FIC",
    write_disposition="replace",
    primary_keys=PRIMARY_KEY_CONFIG["LIVENT"]["keys"],
    column_hints=get_column_hints("LIVENT"),
    use_chunked=True,
    libraries="BXNEGO_FIC;JADE_FIC",
)

# ─── BXNEGO_FIC sources ───────────────────────────────────────────

db2_cdeent_bx_source = create_db2_dlt_source(
    table_name="CDEENT",
    target_name=TABLE_CLASSIFICATIONS["CDEENTBX"]["target_name"],
    schema="BXNEGO_FIC",
    write_disposition="merge",
    primary_keys=PRIMARY_KEY_CONFIG["CDEENT"]["keys"],
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_cdelig_source = create_db2_dlt_source(
    table_name="CDELIG",
    target_name=TABLE_CLASSIFICATIONS["CDELIG"]["target_name"],
    schema="BXNEGO_FIC",
    write_disposition="merge",
    primary_keys=PRIMARY_KEY_CONFIG["CDELIG"]["keys"],
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_cdelig_bx_source = create_db2_dlt_source(
    table_name="CDELIG",
    target_name=TABLE_CLASSIFICATIONS["CDELIGBX"]["target_name"],
    schema="BXNEGO_FIC",
    write_disposition="replace",
    primary_keys=PRIMARY_KEY_CONFIG["CDELIG"]["keys"],
    use_chunked=True,
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_devent_bx_source = create_db2_dlt_source(
    table_name="DEVENT",
    target_name=TABLE_CLASSIFICATIONS["DEVENTBX"]["target_name"],
    schema="BXNEGO_FIC",
    write_disposition="replace",
    primary_keys=PRIMARY_KEY_CONFIG["DEVENT"]["keys"],
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_facent_bx_source = create_db2_dlt_source(
    table_name="FACENT",
    target_name=TABLE_CLASSIFICATIONS["FACENTBX"]["target_name"],
    schema="BXNEGO_FIC",
    write_disposition="merge",
    primary_keys=PRIMARY_KEY_CONFIG["FACENT"]["keys"],
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_faclig_bx_source = create_db2_dlt_source(
    table_name="FACLIG",
    target_name=TABLE_CLASSIFICATIONS["FACLIGBX"]["target_name"],
    schema="BXNEGO_FIC",
    write_disposition="replace",
    primary_keys=PRIMARY_KEY_CONFIG["FACLIG"]["keys"],
    use_chunked=True,
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_livent_bx_source = create_db2_dlt_source(
    table_name="LIVENT",
    target_name=TABLE_CLASSIFICATIONS["LIVENTBX"]["target_name"],
    schema="BXNEGO_FIC",
    write_disposition="replace",
    primary_keys=PRIMARY_KEY_CONFIG["LIVENT"]["keys"],
    use_chunked=True,
    libraries="BXNEGO_FIC;JADE_FIC",
)

# ─── JADE_FIC sources ─────────────────────────────────────────────

db2_tiers_source = create_db2_dlt_source(
    table_name="TIERS",
    target_name=TABLE_CLASSIFICATIONS["TIERS"]["target_name"],
    schema="JADE_FIC",
    write_disposition="replace",
    primary_keys=PRIMARY_KEY_CONFIG["TIERS"]["keys"],
    libraries="BXNEGO_FIC;JADE_FIC",
)

db2_jadirium_source = create_db2_dlt_source(
    table_name="JADIRIUM",
    target_name=TABLE_CLASSIFICATIONS["JADIRIUM"]["target_name"],
    schema="JADE_FIC",
    write_disposition="replace",
    libraries="BXNEGO_FIC;JADE_FIC",
)
