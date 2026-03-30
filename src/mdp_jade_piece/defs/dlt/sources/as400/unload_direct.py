import os
import pyodbc
import pandas as pd
import snowflake.connector
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import logging
import tempfile

from ...configs import TABLE_CLASSIFICATIONS, PRIMARY_KEY_CONFIG, DATE_COLUMN_MAPPING
from mdp_common.vault import get_secret

logger = logging.getLogger(__name__)


def get_as400_odbc_connection():
    """Connexion AS400 via ODBC — plus performant que JDBC."""
    host     = os.getenv("DB2_HOST", "AS400.jadelmas.com")
    user     = os.getenv("AS400_USER", "negoce")
    password = os.getenv("AS400_PASSWORD", "negoce")

    return pyodbc.connect(
        f"DRIVER={{IBM i Access ODBC Driver}};"
        f"SYSTEM={host};"
        f"UID={user};"
        f"PWD={password};"
        f"Naming=1;"
        f"UNICODESQL=1;"
        f"CCSID=1208;"
    )


def get_snowflake_connection(schema_name: str):
    """Connexion Snowflake — même pattern que mdp_common/dagster/resources.py."""
    account  = os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__HOST", "")
    user     = os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME", "")
    database = os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE", "")
    warehouse= os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE", "")

    password_sf = get_secret("mdp/snowflake", "password") or os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD", "")

    kwargs = dict(
        account=account,
        user=user,
        warehouse=warehouse,
        database=database,
        schema=schema_name.upper(),
    )

    #if private_key:
    #    kwargs["private_key"] = private_key
    #else:
    kwargs["password"] = password_sf

    return snowflake.connector.connect(**kwargs)

def get_snowflake_type(pandas_dtype) -> str:
    """Convertit un type pandas en type Snowflake."""
    dtype_str = str(pandas_dtype)
    if "int" in dtype_str:
        return "NUMBER"
    elif "float" in dtype_str:
        return "FLOAT"
    elif "datetime" in dtype_str:
        return "TIMESTAMP_NTZ"
    elif "bool" in dtype_str:
        return "BOOLEAN"
    else:
        return "VARCHAR"


def create_table_if_not_exists(
    sf_cursor,
    target_table_name: str,
    sample_df: pd.DataFrame,
    primary_keys: list,
    context,
):
    """
    Crée la table Snowflake si elle n'existe pas,
    en inférant les types depuis le premier chunk pandas.
    """
    # Génère les colonnes depuis le schéma du DataFrame
    columns_def = ", ".join([
        f"{col.upper()} {get_snowflake_type(dtype)}"
        for col, dtype in sample_df.dtypes.items()
    ])

    # Ajoute les colonnes de metadata comme mdp_common
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table_name.upper()} (
            {columns_def}
        )
    """

    context.log.info(f"📋 Creating table if not exists: {target_table_name}")
    sf_cursor.execute(create_sql)
    context.log.info(f"✅ Table {target_table_name} ready")

def extract_and_stage_to_snowflake(
    context,
    table_name: str,
    target_table_name: str,
    schema_name: str,
    library: str = "NEGOCE_FIC",
    date_column: str = None,
    days_back: int = 7,
    load_mode: str = "full",
    chunk_size: int = 100000,
) -> Dict[str, Any]:
    """
    Extrait depuis AS400 via ODBC et charge dans Snowflake via PUT/COPY INTO.
    Équivalent de unload_direct pour Informix dans mdp_common.

    Flow:
        AS400 (ODBC) → CSV local → PUT Snowflake Stage → COPY INTO table
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stage_name = f"@%{target_table_name.upper()}"
    #stage_name = "@AS400_STAGE"
    tmp_dir = Path(tempfile.mkdtemp(prefix="as400_"))

    # Filtre de date
    where_clause = ""
    if load_mode != "full" and date_column:
        date_filter = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        where_clause = f"WHERE {date_column} >= '{date_filter}'"

    query = f"SELECT * FROM {library}/{table_name} {where_clause}"

    context.log.info("=" * 60)
    context.log.info(f"🚀 AS400 ODBC → Snowflake COPY INTO")
    context.log.info(f"Table: {library}/{table_name} → {schema_name}.{target_table_name}")
    context.log.info(f"Mode: {load_mode} | Query: {query}")
    context.log.info("=" * 60)

    # Connexions
    as400_conn = get_as400_odbc_connection()
    sf_conn    = get_snowflake_connection(schema_name)
    sf_cursor  = sf_conn.cursor()

    csv_files = []
    total_rows = 0
    chunk_num  = 0

    try:
        context.log.info("📡 Extracting from AS400 via ODBC...")

        # Crée la table Snowflake si elle n'existe pas encore
        # DLT s'en charge normalement mais on s'assure qu'elle existe
        # pour le stage interne (@%)
        
        # Extraction par chunks via pandas
        for chunk in pd.read_sql(query, as400_conn, chunksize=chunk_size):
            chunk_num += 1
            rows_in_chunk = len(chunk)
            total_rows += rows_in_chunk

            if chunk_num == 1:
                create_table_if_not_exists(
                    sf_cursor=sf_cursor,
                    target_table_name=target_table_name,
                    sample_df=chunk,
                    primary_keys=PRIMARY_KEY_CONFIG.get(table_name, {}).get("keys", []),
                    context=context,
                )

            # Sauvegarde chunk en CSV local
            csv_file = str(tmp_dir / f"{table_name}_{timestamp}_part{chunk_num}.csv")
            chunk.to_csv(csv_file, index=False, header=(chunk_num == 1))
            csv_files.append(csv_file)

            context.log.info(
                f"📦 Chunk {chunk_num}: {rows_in_chunk:,} rows → {csv_file}"
            )

            # PUT vers le stage Snowflake interne
            sf_cursor.execute(
                f"PUT file://{csv_file} {stage_name} "
                f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )
            context.log.info(f"✅ PUT chunk {chunk_num} → {stage_name}")

            # Supprime le CSV local après PUT
            Path(csv_file).unlink()

        as400_conn.close()
        context.log.info(f"✅ Extraction complete: {total_rows:,} rows in {chunk_num} chunks")

        # COPY INTO Snowflake
        context.log.info(f"📥 COPY INTO {target_table_name}...")

        primary_keys = PRIMARY_KEY_CONFIG.get(table_name, {}).get("keys", [])

        if load_mode == "full":
            # Truncate + insert pour full refresh
            sf_cursor.execute(f"TRUNCATE TABLE {target_table_name.upper()}")
            sf_cursor.execute(
                f"COPY INTO {target_table_name.upper()} "
                f"FROM {stage_name} "
                f"FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 "
                f"FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
                f"NULL_IF = ('', 'NULL')) "
                f"PURGE = TRUE"   # ← supprime les fichiers du stage après COPY
            )
        else:
            # Merge pour incremental — via table temporaire
            tmp_table = f"{target_table_name.upper()}_TMP_{timestamp}"

            sf_cursor.execute(
                f"CREATE TEMP TABLE {tmp_table} LIKE {target_table_name.upper()}"
            )
            sf_cursor.execute(
                f"COPY INTO {tmp_table} "
                f"FROM {stage_name} " #Le stage name est celui de la table finale et donc different du tmp_table le copy ne fonctionnera pas à Corriger
                f"FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 "
                f"FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
                f"NULL_IF = ('', 'NULL')) "
                f"PURGE = TRUE"
            )

            # MERGE depuis table temporaire
            merge_keys = " AND ".join(
                [f"target.{k} = source.{k}" for k in primary_keys]
            )
            sf_cursor.execute(f"SELECT COUNT(*) FROM {tmp_table}")
            staged_rows = sf_cursor.fetchone()[0]
            context.log.info(f"📊 Staged rows to merge: {staged_rows:,}")

            sf_cursor.execute(
                f"MERGE INTO {target_table_name.upper()} AS target "
                f"USING {tmp_table} AS source "
                f"ON {merge_keys} "
                f"WHEN MATCHED THEN UPDATE SET * "
                f"WHEN NOT MATCHED THEN INSERT *"
            )
            sf_cursor.execute(f"DROP TABLE IF EXISTS {tmp_table}")

        sf_conn.commit()
        sf_cursor.close()
        sf_conn.close()

        context.log.info(f"✅ COPY INTO complete: {total_rows:,} rows loaded")

        return {
            "rows_loaded": total_rows,
            "chunks": chunk_num,
            "table": target_table_name,
            "mode": load_mode,
        }

    except Exception as e:
        context.log.error(f"❌ Failed: {e}")
        # Nettoyage fichiers locaux si erreur
        for f in csv_files:
            if Path(f).exists():
                Path(f).unlink()
        raise
    finally:
        tmp_dir.rmdir() if tmp_dir.exists() else None