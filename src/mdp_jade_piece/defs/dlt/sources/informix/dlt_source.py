import dlt
from typing import Iterator, Dict, Any, Optional, Literal
from .extract import extract_with_arrow
from mdp_common.dlt.extract import get_incremental_date_filter
from mdp_common.snowflake import SnowflakeStageManager
from ...configs import (
    DATE_COLUMN_MAPPING,
    TABLE_CLASSIFICATIONS,
    DATASET_NAME,
    PRIMARY_KEY_CONFIG,
)
from dagster import AssetExecutionContext

######## sav_lor ###################################
@dlt.resource(
    name="sav_lor",
    table_name=TABLE_CLASSIFICATIONS["sav_lor"]["target_name"],
    write_disposition="merge",
    primary_key=[PRIMARY_KEY_CONFIG["sav_lor"]["keys"]],
    merge_key=[PRIMARY_KEY_CONFIG["sav_lor"]["merge_keys"]],
    columns={
        "slor_soc": {"description": "Company number "},
        "slor_succ": {"description": "Company branch"},
    },
)
def get_sav_lor_data(
    context: AssetExecutionContext,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Extract repair order line data with Arrow optimization"""

    table_name = "sav_lor"

    if date_filter is None:
        date_filter = get_incremental_date_filter(
            table_name, DATE_COLUMN_MAPPING, load_mode, days_back
        )

    query = f"""
        SELECT
            slor_soc, slor_succ, slor_numor, slor_datel, slor_nolign,
            slor_nogrp, slor_typlig, slor_const, slor_ref, slor_refnb,
            slor_couhs, slor_imp, slor_constp, slor_refp, slor_refpnb,
            slor_desi, slor_famc, slor_fams1, slor_fams2, slor_codg,
            slor_codfac, slor_cremvte, slor_txtva, slor_stat, slor_nbdecq,
            slor_histo, slor_histm, slor_matric, slor_fong, slor_natop,
            slor_servcrt, slor_servdeb, slor_succdeb, slor_famcptm,
            slor_pxvteht, slor_remise, slor_pxntheo, slor_pxnreel,
            slor_pmp, slor_pxcess, slor_qte, slor_qteres, slor_qterel,
            slor_qterea, slor_numfac, slor_pos, slor_natcm, slor_numcf,
            slor_datecf, slor_typcf, slor_noligncm, slor_qrec, slor_lienmo,
            slor_usr, slor_typeor, slor_numcli, slor_nolmait, slor_nivnmc,
            slor_impnmc, slor_pxvtettc, slor_pxnttc, slor_qteaff,
            slor_id_externe, slor_techn, slor_codret, slor_qtewait,
            slor_datealloc, slor_excep, slor_greap, slor_qtedisp,
            slor_datec, slor_vend,
            CURRENT AS integration_date,
            '{table_name}' AS source_table_name
        FROM ie_delmas:informix.{table_name}
    """

    if date_filter:
        date_column = DATE_COLUMN_MAPPING[table_name]["column"]
        query += f" WHERE {date_column} >= TO_DATE('{date_filter}', '%Y-%m-%d')"

    # query += " ORDER BY asoc_num"

    context.log.info(f"🐬 Query: {query}")

    # Extract with Arrow
    arrow_table = extract_with_arrow(context, table_name, date_filter, query, get_count)

    # Persist to Snowflake stage
    if persist_parquet and arrow_table.num_rows > 0:
        try:
            stage_manager = SnowflakeStageManager(DATASET_NAME)
            stage_path = stage_manager.upload_parquet_to_stage(
                arrow_table, table_name, load_mode
            )
            print(f"📦 Parquet archived at: {stage_path}")
        except Exception as e:
            print(f"⚠️  Could not persist Parquet: {e}")

    # Yield data for DLT
    for batch in arrow_table.to_batches(max_chunksize=50000):
        yield from batch.to_pylist()


@dlt.source
def informix_sav_lor_source(
    context: AssetExecutionContext = None,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
):
    """Informix source with sav_lor table (company master data)"""
    return get_sav_lor_data(
        context=context,
        load_mode=load_mode,
        days_back=days_back,
        date_filter=date_filter,
        get_count=get_count,
        persist_parquet=persist_parquet,
    )

######## end sav_lor ###################################

######## sav_eor  ###################################

@dlt.resource(
    name="sav_eor",
    table_name=TABLE_CLASSIFICATIONS["sav_eor"]["target_name"],
    write_disposition="merge",
    primary_key=[PRIMARY_KEY_CONFIG["sav_eor"]["keys"]],
    merge_key=[PRIMARY_KEY_CONFIG["sav_eor"]["merge_keys"]],
    columns={
        "seor_soc": {"description": "Company number "},
        "seor_succ": {"description": "Company branch"},
    },
)
def get_sav_eor_data(
    context: AssetExecutionContext,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Extract repair order line data with Arrow optimization"""

    table_name = "sav_eor"

    if date_filter is None:
        date_filter = get_incremental_date_filter(
            table_name, DATE_COLUMN_MAPPING, load_mode, days_back
        )

    query = f"""
        SELECT seor_soc, seor_succ, seor_numor, seor_dateor
              ,seor_typeor, seor_ope, seor_delai, seor_lib
              ,seor_numcli, seor_numadr, seor_vend1, seor_vend2
              ,seor_ptt, seor_tarif, seor_fong, seor_codremise
              ,seor_remglob, seor_txesc, seor_tva, seor_modp
              ,seor_echeance, seor_publi, seor_texte, seor_frsfac
              ,seor_nbexpl, seor_edifac, seor_natop, seor_servdeb
              ,seor_succdeb, seor_succdev, seor_numdev, seor_nummat
              ,seor_acompte, seor_posac, seor_datefin, seor_pos
              ,seor_usr,seor_datedev, seor_grpfac, seor_servcrt
              ,seor_clicompt, seor_nomcli, seor_numfacann, seor_panne
              ,seor_techn,seor_serv, seor_lsa, seor_matric, seor_tpsprv
              ,seor_datepla, seor_externe, seor_codemo, seor_status
              ,seor_typitv, seor_dateappel, seor_heureappel,seor_datedebut
              ,seor_heuredebut, seor_delaiint, seor_numclifi, seor_tel
              ,seor_numresp, seor_port, seor_transp,seor_went_id, seor_devise
              ,seor_cours, seor_refdem, seor_heureor,seor_heuredelai
              ,seor_document, seor_interv_dev, seor_numor_lie,seor_numor_seq
              ,seor_zonetva, seor_nbjoursimmo, seor_finvalidevis
              ,seor_datesormat, seor_codeprojet
              ,CURRENT AS integration_date
              ,'{table_name}' AS source_table_name
        FROM ie_delmas:informix.{table_name}
    """

    if date_filter:
        date_column = DATE_COLUMN_MAPPING[table_name]["column"]
        query += f" WHERE {date_column} >= TO_DATE('{date_filter}', '%Y-%m-%d')"

    # query += " ORDER BY asoc_num"

    context.log.info(f"🐬 Query: {query}")

    # Extract with Arrow
    arrow_table = extract_with_arrow(context, table_name, date_filter, query, get_count)

    # Persist to Snowflake stage
    if persist_parquet and arrow_table.num_rows > 0:
        try:
            stage_manager = SnowflakeStageManager(DATASET_NAME)
            stage_path = stage_manager.upload_parquet_to_stage(
                arrow_table, table_name, load_mode
            )
            print(f"📦 Parquet archived at: {stage_path}")
        except Exception as e:
            print(f"⚠️  Could not persist Parquet: {e}")

    # Yield data for DLT
    for batch in arrow_table.to_batches(max_chunksize=50000):
        yield from batch.to_pylist()


@dlt.source
def informix_sav_eor_source(
    context: AssetExecutionContext = None,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
):
    """Informix source with sav_eor table (company master data)"""
    return get_sav_eor_data(
        context=context,
        load_mode=load_mode,
        days_back=days_back,
        date_filter=date_filter,
        get_count=get_count,
        persist_parquet=persist_parquet,
    )
####################### end sav_eor ###################################

####################### sav_itv ###################################

@dlt.resource(
    name="sav_itv",
    table_name=TABLE_CLASSIFICATIONS["sav_itv"]["target_name"],
    write_disposition="merge",
    primary_key=[PRIMARY_KEY_CONFIG["sav_itv"]["keys"]],
    merge_key=[PRIMARY_KEY_CONFIG["sav_itv"]["merge_keys"]],
    columns={
        "sitv_soc": {"description": "Company number "},
        "sitv_succ": {"description": "Company branch"},
    },
)
def get_sav_itv_data(
    context: AssetExecutionContext,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Extract repair order line data with Arrow optimization"""

    table_name = "sav_itv"

    if date_filter is None:
        date_filter = get_incremental_date_filter(
            table_name, DATE_COLUMN_MAPPING, load_mode, days_back
        )

    query = f"""
        SELECT sitv_soc, sitv_succ, sitv_numor, sitv_interv, sitv_numcli
              ,sitv_typeor, sitv_natop, sitv_servcrt, sitv_servdeb
              ,sitv_succdeb, sitv_contrat, sitv_depasst, sitv_nodoss
              ,sitv_datdeb, sitv_hredeb, sitv_datfin, sitv_hrefin
              ,sitv_comment, sitv_numdev, sitv_codedi, sitv_nomcli
              ,sitv_tarif, sitv_fong, sitv_codremise, sitv_remglob
              ,sitv_txesc, sitv_tva, sitv_modp, sitv_echeance,sitv_frsfac
              ,sitv_pos, sitv_grpfac, sitv_numfacann,sitv_succdev,sitv_techn
              ,sitv_tpsprv, sitv_datepla,sitv_ordre,sitv_termine,sitv_codemo
              ,sitv_typitv,sitv_port,sitv_transp, sitv_datm, sitv_constp
              ,sitv_refp, sitv_interv_dev,sitv_codelibre1, sitv_codelibre2
              ,sitv_codelibre3,sitv_codelibre4, sitv_edifac, sitv_zonetva
              ,sitv_rang, sitv_pick_regle
              ,CURRENT AS integration_date
              ,'{table_name}' AS source_table_name
        FROM ie_delmas:informix.{table_name}
    """

    if date_filter:
        date_column = DATE_COLUMN_MAPPING[table_name]["column"]
        query += f" WHERE {date_column} >= TO_DATE('{date_filter}', '%Y-%m-%d')"

    # query += " ORDER BY asoc_num"

    context.log.info(f"🐬 Query: {query}")

    # Extract with Arrow
    arrow_table = extract_with_arrow(context, table_name, date_filter, query, get_count)

    # Persist to Snowflake stage
    if persist_parquet and arrow_table.num_rows > 0:
        try:
            stage_manager = SnowflakeStageManager(DATASET_NAME)
            stage_path = stage_manager.upload_parquet_to_stage(
                arrow_table, table_name, load_mode
            )
            print(f"📦 Parquet archived at: {stage_path}")
        except Exception as e:
            print(f"⚠️  Could not persist Parquet: {e}")

    # Yield data for DLT
    for batch in arrow_table.to_batches(max_chunksize=50000):
        yield from batch.to_pylist()


@dlt.source
def informix_sav_itv_source(
    context: AssetExecutionContext = None,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
):
    """Informix source with sav_itv table (company master data)"""
    return get_sav_itv_data(
        context=context,
        load_mode=load_mode,
        days_back=days_back,
        date_filter=date_filter,
        get_count=get_count,
        persist_parquet=persist_parquet,
    )
######## end sav_eor ###################################

####################### sip_lnk ###################################

@dlt.resource(
    name="sip_lnk",
    table_name=TABLE_CLASSIFICATIONS["sip_lnk"]["target_name"],
    write_disposition="merge",
    primary_key=[PRIMARY_KEY_CONFIG["sip_lnk"]["keys"]],
    merge_key=[PRIMARY_KEY_CONFIG["sip_lnk"]["merge_keys"]],
    columns={
        "slnk_pksoc": {"description": "Company number "},
        "slnk_pksucc": {"description": "Company branch"},
    },
)
def get_sip_lnk_data(
    context: AssetExecutionContext,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Extract repair order line data with Arrow optimization"""

    table_name = "sip_lnk"

    if date_filter is None:
        date_filter = get_incremental_date_filter(
            table_name, DATE_COLUMN_MAPPING, load_mode, days_back
        )

    query = f"""
        SELECT slnk_id, slnk_sfrm_id, slnk_tabname, slnk_tabname, slnk_pksucc
              ,slnk_pk1, slnk_pk2, slnk_pk3, slnk_pk4, slnk_pk5, slnk_date1
              ,slnk_date2, slnk_date3, slnk_date4, slnk_date5, slnk_date6
              ,slnk_date7, slnk_date8, slnk_date9, slnk_date10, slnk_date11
              ,slnk_date12, slnk_date13, slnk_date14, slnk_date15, slnk_date16
              ,slnk_date17, slnk_date18, slnk_date19, slnk_date20, slnk_date21
              ,slnk_date22, slnk_date23, slnk_date24, slnk_date25, slnk_date26
              ,slnk_date27, slnk_date28, slnk_date29, slnk_date30, slnk_date31
              ,slnk_date32, slnk_date33, slnk_date34, slnk_date35, slnk_date36
              ,slnk_date37, slnk_date38, slnk_date39, slnk_date40, slnk_date41
              ,slnk_date42, slnk_date43, slnk_date44, slnk_date45, slnk_date46
              ,slnk_date47, slnk_date48, slnk_date49, slnk_date50, slnk_num1
              ,slnk_num2, slnk_num3, slnk_num4, slnk_num5, slnk_num6, slnk_num7
              ,slnk_num8, slnk_num9, slnk_num10, slnk_num11, slnk_num12
              ,slnk_num13, slnk_num14, slnk_num15, slnk_num16, slnk_num17
              ,slnk_num18, slnk_num19, slnk_num20, slnk_num21, slnk_num22
              ,slnk_num23, slnk_num24, slnk_num25, slnk_num26, slnk_num27
              ,slnk_num28, slnk_num29, slnk_num30, slnk_num31, slnk_num32
              ,slnk_num33, slnk_num34, slnk_num35, slnk_num36, slnk_num37
              ,slnk_num38, slnk_num39, slnk_num40, slnk_num41, slnk_num42
              ,slnk_num43, slnk_num44, slnk_num45, slnk_num46, slnk_num47
              ,slnk_num48, slnk_num49, slnk_num50, slnk_alpha1, slnk_alpha2
              ,slnk_alpha3, slnk_alpha4, slnk_alpha5, slnk_alpha6, slnk_alpha7
              ,slnk_alpha8, slnk_alpha9, slnk_alpha10, slnk_alpha11, slnk_alpha12
              ,slnk_alpha13, slnk_alpha14, slnk_alpha15, slnk_alpha16, slnk_alpha17
              ,slnk_alpha18, slnk_alpha19, slnk_alpha20, slnk_alpha21, slnk_alpha22
              ,slnk_alpha23, slnk_alpha24, slnk_alpha25, slnk_alpha26, slnk_alpha27
              ,slnk_alpha28, slnk_alpha29, slnk_alpha30, slnk_alpha31, slnk_alpha32
              ,slnk_alpha33, slnk_alpha34, slnk_alpha35, slnk_alpha36, slnk_alpha37
              ,slnk_alpha38, slnk_alpha39, slnk_alpha40, slnk_alpha41, slnk_alpha42
              ,slnk_alpha43, slnk_alpha44, slnk_alpha45, slnk_alpha46, slnk_alpha47
              ,slnk_alpha48, slnk_alpha49, slnk_alpha50, slnk_datetime1, slnk_datetime2
              ,slnk_datetime3, slnk_datetime4, slnk_datetime5, slnk_datetime6
              ,slnk_datetime7, slnk_datetime8, slnk_datetime9, slnk_datetime10
              ,CURRENT AS integration_date
              ,'{table_name}' AS source_table_name
        FROM ie_delmas:regix.{table_name}
    """

    if date_filter:
        date_column = DATE_COLUMN_MAPPING[table_name]["column"]
        query += f" WHERE {date_column} >= TO_DATE('{date_filter}', '%Y-%m-%d')"

    # query += " ORDER BY asoc_num"

    context.log.info(f"🐬 Query: {query}")

    # Extract with Arrow
    arrow_table = extract_with_arrow(context, table_name, date_filter, query, get_count)

    # Persist to Snowflake stage
    if persist_parquet and arrow_table.num_rows > 0:
        try:
            stage_manager = SnowflakeStageManager(DATASET_NAME)
            stage_path = stage_manager.upload_parquet_to_stage(
                arrow_table, table_name, load_mode
            )
            print(f"📦 Parquet archived at: {stage_path}")
        except Exception as e:
            print(f"⚠️  Could not persist Parquet: {e}")

    # Yield data for DLT
    for batch in arrow_table.to_batches(max_chunksize=50000):
        yield from batch.to_pylist()


@dlt.source
def regix_sip_lnk_source(
    context: AssetExecutionContext = None,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
):
    """Informix source with sip_lnk table (company master data)"""
    return get_sip_lnk_data(
        context=context,
        load_mode=load_mode,
        days_back=days_back,
        date_filter=date_filter,
        get_count=get_count,
        persist_parquet=persist_parquet,
    )
######## end sip_lnk ###################################

####################### sav_liv ###################################

@dlt.resource(
    name="sav_liv",
    table_name=TABLE_CLASSIFICATIONS["sav_liv"]["target_name"],
    write_disposition="merge",
    primary_key=[PRIMARY_KEY_CONFIG["sav_liv"]["keys"]],
    merge_key=[PRIMARY_KEY_CONFIG["sav_liv"]["merge_keys"]],
    columns={
        "sliv_soc": {"description": "Company number "},
        "sliv_succ": {"description": "Company branch"},
    },
)
def get_sav_liv_data(
    context: AssetExecutionContext,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Extract repair order line data with Arrow optimization"""

    table_name = "sav_liv"

    if date_filter is None:
        date_filter = get_incremental_date_filter(
            table_name, DATE_COLUMN_MAPPING, load_mode, days_back
        )

    query = f"""
        SELECT sliv_soc, sliv_succ, sliv_numor, sliv_nolign
              ,sliv_qteres, sliv_qteliv, sliv_natdep, sliv_codstock
	          ,sliv_numliv, sliv_date, sliv_pos, sliv_usr, sliv_natdepc
	          , sliv_codstockc, sliv_nogrp, sliv_document
              ,CURRENT AS integration_date
              ,'{table_name}' AS source_table_name
        FROM ie_delmas:informix.{table_name}
    """

    if date_filter:
        date_column = DATE_COLUMN_MAPPING[table_name]["column"]
        query += f" WHERE {date_column} >= TO_DATE('{date_filter}', '%Y-%m-%d')"

    # query += " ORDER BY asoc_num"

    context.log.info(f"🐬 Query: {query}")

    # Extract with Arrow
    arrow_table = extract_with_arrow(context, table_name, date_filter, query, get_count)

    # Persist to Snowflake stage
    if persist_parquet and arrow_table.num_rows > 0:
        try:
            stage_manager = SnowflakeStageManager(DATASET_NAME)
            stage_path = stage_manager.upload_parquet_to_stage(
                arrow_table, table_name, load_mode
            )
            print(f"📦 Parquet archived at: {stage_path}")
        except Exception as e:
            print(f"⚠️  Could not persist Parquet: {e}")

    # Yield data for DLT
    for batch in arrow_table.to_batches(max_chunksize=50000):
        yield from batch.to_pylist()


@dlt.source
def informix_sav_liv_source(
    context: AssetExecutionContext = None,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
):
    """Informix source with sav_liv table (company master data)"""
    return get_sav_liv_data(
        context=context,
        load_mode=load_mode,
        days_back=days_back,
        date_filter=date_filter,
        get_count=get_count,
        persist_parquet=persist_parquet,
    )
######## end sav_liv ###################################

####################### dpc_fac ###################################

@dlt.resource(
    name="dpc_fac",
    table_name=TABLE_CLASSIFICATIONS["dpc_fac"]["target_name"],
    write_disposition="merge",
    primary_key=[PRIMARY_KEY_CONFIG["dpc_fac"]["keys"]],
    merge_key=[PRIMARY_KEY_CONFIG["dpc_fac"]["merge_keys"]],
    columns={
        "dfac_numfac": {"description": "invoice number"},
        "dfac_datefac": {"description": "Invoice date"},
        "dfac_numcli": {"description": "Customer number"},
        "dfac_numfacm": {"description": "Master invoices number"},
        "dfac_pos": {"description": "Status Invoice"},
    },
)
def get_dpc_fac_data(
    context: AssetExecutionContext,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Extract repair order line data with Arrow optimization"""

    table_name = "dpc_fac"

    if date_filter is None:
        date_filter = get_incremental_date_filter(
            table_name, DATE_COLUMN_MAPPING, load_mode, days_back
        )

    query = f"""
        SELECT dfac_soc, dfac_succ, dfac_serv, dfac_numfac, dfac_datefac, dfac_numcli, dfac_pos, 
        dfac_numfacm, dfac_code, dfac_devise, dfac_cours, dfac_hashcode
        ,CURRENT AS integration_date
        ,'{table_name}' AS source_table_name
        FROM ie_delmas:informix.{table_name}
    """

    if date_filter:
        date_column = DATE_COLUMN_MAPPING[table_name]["column"]
        query += f" WHERE {date_column} >= TO_DATE('{date_filter}', '%Y-%m-%d')"

    # query += " ORDER BY asoc_num"

    context.log.info(f"🐬 Query: {query}")

    # Extract with Arrow
    arrow_table = extract_with_arrow(context, table_name, date_filter, query, get_count)

    # Persist to Snowflake stage
    if persist_parquet and arrow_table.num_rows > 0:
        try:
            stage_manager = SnowflakeStageManager(DATASET_NAME)
            stage_path = stage_manager.upload_parquet_to_stage(
                arrow_table, table_name, load_mode
            )
            print(f"📦 Parquet archived at: {stage_path}")
        except Exception as e:
            print(f"⚠️  Could not persist Parquet: {e}")

    # Yield data for DLT
    for batch in arrow_table.to_batches(max_chunksize=50000):
        yield from batch.to_pylist()


@dlt.source
def informix_dpc_fac_source(
    context: AssetExecutionContext = None,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
):
    """Informix source with dpc_fac table """
    return get_dpc_fac_data(
        context=context,
        load_mode=load_mode,
        days_back=days_back,
        date_filter=date_filter,
        get_count=get_count,
        persist_parquet=persist_parquet,
    )
######## end dpc_fac ###################################

####################### dpc_fcc ###################################

@dlt.resource(
    name="dpc_fcc",
    table_name=TABLE_CLASSIFICATIONS["dpc_fcc"]["target_name"],
    write_disposition="merge",
    primary_key=[PRIMARY_KEY_CONFIG["dpc_fcc"]["keys"]],
    merge_key=[PRIMARY_KEY_CONFIG["dpc_fcc"]["merge_keys"]],
    columns={
        "dfcc_numfcc": {"description": "invoice number for cession"},
        "dfcc_datefac": {"description": "Invoice date"},
        "dfcc_numcli": {"description": "Customer number"},
        "dfcc_pos": {"description": "Status Invoice"},
        "dfcc_natcess": {"description": "Nature of cession"},
    },
)
def get_dpc_fcc_data(
    context: AssetExecutionContext,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Extract repair order line data with Arrow optimization"""

    table_name = "dpc_fcc"

    if date_filter is None:
        date_filter = get_incremental_date_filter(
            table_name, DATE_COLUMN_MAPPING, load_mode, days_back
        )

    query = f"""
        SELECT dfcc_soc, dfcc_succ, dfcc_serv, dfcc_numfcc, dfcc_datefac, 
        dfcc_numcli, dfcc_pos, dfcc_natcess, dfcc_document
        ,CURRENT AS integration_date
        ,'{table_name}' AS source_table_name
        FROM ie_delmas:informix.{table_name}
    """

    if date_filter:
        date_column = DATE_COLUMN_MAPPING[table_name]["column"]
        query += f" WHERE {date_column} >= TO_DATE('{date_filter}', '%Y-%m-%d')"

    # query += " ORDER BY dfcc_numfac, dfcc_soc"

    context.log.info(f"🐬 Query: {query}")

    # Extract with Arrow
    arrow_table = extract_with_arrow(context, table_name, date_filter, query, get_count)

    # Persist to Snowflake stage
    if persist_parquet and arrow_table.num_rows > 0:
        try:
            stage_manager = SnowflakeStageManager(DATASET_NAME)
            stage_path = stage_manager.upload_parquet_to_stage(
                arrow_table, table_name, load_mode
            )
            print(f"📦 Parquet archived at: {stage_path}")
        except Exception as e:
            print(f"⚠️  Could not persist Parquet: {e}")

    # Yield data for DLT
    for batch in arrow_table.to_batches(max_chunksize=50000):
        yield from batch.to_pylist()


@dlt.source
def informix_dpc_fcc_source(
    context: AssetExecutionContext = None,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
):
    """Informix source with dpc_fcc table """
    return get_dpc_fcc_data(
        context=context,
        load_mode=load_mode,
        days_back=days_back,
        date_filter=date_filter,
        get_count=get_count,
        persist_parquet=persist_parquet,
    )
######## end dpc_fcc ###################################

####################### dpc_pie ###################################

@dlt.resource(
    name="dpc_pie",
    table_name=TABLE_CLASSIFICATIONS["dpc_pie"]["target_name"],
    write_disposition="merge",
    primary_key=[PRIMARY_KEY_CONFIG["dpc_pie"]["keys"]],
    merge_key=[PRIMARY_KEY_CONFIG["dpc_pie"]["merge_keys"]],
    columns={
        "dpie_numfac": {"description": "invoices number"},
        "dpie_base": {"description": "Base amount excluding tax"},
        "dpie_port": {"description": "Port amount"},
    },
)
def get_dpc_pie_data(
    context: AssetExecutionContext,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
) -> Iterator[Dict[str, Any]]:
    """Extract footer of service invoice data with Arrow optimization"""

    table_name = "dpc_pie"

    if date_filter is None:
        date_filter = get_incremental_date_filter(
            table_name, DATE_COLUMN_MAPPING, load_mode, days_back
        )

    query = f"""
        SELECT dpie_soc, dpie_succ, dpie_serv, dpie_numfac, dpie_codetva, dpie_base, 
        dpie_remise, dpie_escompte, dpie_port, dpie_ht, dpie_tva, dpie_ttc, dpie_mttvanpr
        ,CURRENT AS integration_date
        ,'{table_name}' AS source_table_name
        FROM ie_delmas:informix.{table_name}
    """

    if date_filter:
        date_column = DATE_COLUMN_MAPPING[table_name]["column"]
        query += f" WHERE {date_column} >= TO_DATE('{date_filter}', '%Y-%m-%d')"

    # query += " ORDER BY dpie_numfac, dpie_soc"

    context.log.info(f"🐬 Query: {query}")

    # Extract with Arrow
    arrow_table = extract_with_arrow(context, table_name, date_filter, query, get_count)

    # Persist to Snowflake stage
    if persist_parquet and arrow_table.num_rows > 0:
        try:
            stage_manager = SnowflakeStageManager(DATASET_NAME)
            stage_path = stage_manager.upload_parquet_to_stage(
                arrow_table, table_name, load_mode
            )
            print(f"📦 Parquet archived at: {stage_path}")
        except Exception as e:
            print(f"⚠️  Could not persist Parquet: {e}")

    # Yield data for DLT
    for batch in arrow_table.to_batches(max_chunksize=50000):
        yield from batch.to_pylist()


@dlt.source
def informix_dpc_pie_source(
    context: AssetExecutionContext = None,
    load_mode: Literal["full", "incremental", "last_n_days"] = "incremental",
    days_back: int = 7,
    date_filter: Optional[str] = None,
    get_count: bool = True,
    persist_parquet: bool = True,
):
    """Informix source with dpc_pie table """
    return get_dpc_pie_data(
        context=context,
        load_mode=load_mode,
        days_back=days_back,
        date_filter=date_filter,
        get_count=get_count,
        persist_parquet=persist_parquet,
    )
######## end dpc_pie ###################################