import dlt
from dlt.common.schema.typing import TColumnSchema
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
from ...configs import get_column_hints
from dagster import AssetExecutionContext
from dlt.sources.sql_database import sql_database, sql_table
from dlt.extract.resource import DltResource
from sqlalchemy import create_engine, event, pool
from sqlalchemy.engine import Engine
import urllib.parse
import os
import time
import logging
import unicodedata
import re
import os
#import ibm_db
#import ibm_db_dbi

import jaydebeapi
import os

from pathlib import Path

from datetime import datetime, timedelta

import math
##################################################################################
def get_db2_engine():
    """
    Connexion DB2 locale au projet — sans mdp_common pour l'instant.
    Credentials depuis variables d'environnement ou Vault directement.
    """
    host     = os.getenv("DB2_HOST", "AS400.jadelmas.com")
    port     = os.getenv("DB2_PORT", "446")
    #database = os.getenv("DB2_DATABASE", "NEGOCE_FIC")
    user     = os.getenv("DB2_USER", "negoce")
    password = os.getenv("DB2_PASSWORD", "negoce")


    jdbc_url = f"jdbc:as400://{host};libraries=BXNEGO_FIC;JADE_FIC;naming=system;"   # ← Automatiser le BXNEGO avec une variable
    return jaydebeapi.connect(
        "com.ibm.as400.access.AS400JDBCDriver",  # Driver JTOpen
        jdbc_url,
        [user, password],
        "/home/mboutchoua/projects/mdp-customer/jt400-20.0.7.jar"               # A changer pour la prod
    )

def convert_java_value(value):
    """Convertit les types Java en types Python natifs."""
    if value is None:
        return None

    # Déjà un type Python natif
    if type(value) in (int, float, str, bool):
        return value

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    # Tous les types Java (JDouble, JFloat, JInt, JLong, etc.)
    type_name = value.__class__.__name__
    str_value = str(value)

    try:
        if any(t in type_name for t in ["Double", "Float"]):
            result = float(str_value)
            # Gère NaN et Infinity qui ne sont pas JSON serialisables
            if math.isnan(result) or math.isinf(result):
                return None
            return result

        elif any(t in type_name for t in ["Int", "Long", "Short", "Byte"]):
            return int(float(str_value))

        elif "Boolean" in type_name:
            return str_value.lower() == "true"

        elif any(t in type_name for t in ["Date", "Time", "Timestamp"]):
            return str_value  # Retourne en string, DLT gère la conversion

        else:
            return str_value

    except (ValueError, TypeError):
        return str_value

#def convert_java_value(value):
#    """
#    Safely convert Java objects to Python types
#    """
#    if value is None:
#        return None
#    
#    # Check if it's already a Python type
#    if isinstance(value, (str, int, float, bool, bytes)):
#        return value
#    
#    # Handle Java objects
#    if hasattr(value, '__class__'):
#        class_name = str(value.__class__)
#        
#        # Handle BigDecimal
#        if 'BigDecimal' in class_name:
#            str_val = str(value)
#            try:
#                # Try to preserve precision
#                if '.' in str_val:
#                    return float(str_val)
#                else:
#                    return int(str_val)
#            except:
#                return str_val
#        
#        # Handle Timestamps/Dates
#        elif any(x in class_name for x in ['Timestamp', 'Date', 'Time']):
#            # Return ISO format string
#            return str(value)
#        
#        # Handle Clob/Blob
#        elif 'Clob' in class_name:
#            try:
#                if hasattr(value, 'getCharacterStream'):
#                    reader = value.getCharacterStream()
#                    chars = []
#                    while True:
#                        ch = reader.read()
#                        if ch == -1:
#                            break
#                        chars.append(chr(ch))
#                    return ''.join(chars)
#            except:
#                pass
#            return str(value)
#        
#        # Handle Java strings
#        elif 'String' in class_name or hasattr(value, 'toString'):
#            return str(value)
#    
#    # Last resort - convert to string
#    try:
#        return str(value)
#    except:
#        return repr(value)

BATCH_SIZE_CONFIG = {
    "small":  {"chunk_size": 10000},
    "medium": {"chunk_size": 50000},
    "large":  {"chunk_size": 100000},
}

def fetch_table(schema: str, table_name: str, date_column: str = None, days_back: int = 7, load_mode: str = "incremental"):
    """Extrait une table AS400 et retourne une liste de dicts."""
    conn = get_db2_engine()
    cursor = conn.cursor()

    query = f"SELECT * FROM {schema}/{table_name}"
    if load_mode != "full" and date_column:
        #date_filter = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        query += f" WHERE {date_column} >= '{date_filter}'"

    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # Retourne une liste de dicts — format natif DLT
    return [
        {col: convert_java_value(val) for col, val in zip(columns, row)}
        for row in rows
    ]

def fetch_table_chunked(
    schema: str,
    table_name: str,
    date_column: str = None,
    days_back: int = 7,
    load_mode: str = "incremental",
    chunk_size: int = 50000,  # ← à ajuster selon la mémoire disponible
):
    size = TABLE_CLASSIFICATIONS.get(table_name, {}).get("size", "medium")
    chunk_size = BATCH_SIZE_CONFIG[size]["chunk_size"]
    """Extrait par chunks pour éviter les problèmes mémoire sur les grandes tables."""
    conn = get_db2_engine()
    cursor = conn.cursor()

    query = f"SELECT * FROM {schema}/{table_name}"
    if load_mode != "full" and date_column:
        date_filter = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        query += f" WHERE {date_column} >= '{date_filter}'"

    cursor.execute(query)

    while True:
        rows = cursor.fetchmany(chunk_size)  # ← fetch par batch
        if not rows:
            break

        columns = [desc[0] for desc in cursor.description]
        yield [
            {col: convert_java_value(val) for col, val in zip(columns, row)}
            for row in rows
        ]

    cursor.close()
    conn.close()

@dlt.source
def db2_cdeent_source(
    load_mode: str = "incremental",
    days_back: int = 7,
):
    @dlt.resource(
        name="CDEENT",
        table_name = TABLE_CLASSIFICATIONS["CDEENT"]["target_name"],
        write_disposition="replace",
        primary_key=PRIMARY_KEY_CONFIG["CDEENT"]["keys"],
        columns=get_column_hints("CDEENT"), # Si besoin d'inférer un type de données
    )
    def order_part_header_resource():
        yield fetch_table(
            schema="NEGOCE_FIC",
            table_name="CDEENT",        
            date_column="datecreat",    
            days_back=days_back,
            load_mode=load_mode,
        )

    return order_part_header_resource

@dlt.source
def db2_cdeent_bx_source(
    load_mode: str = "full",
    days_back: int = 7,
):
    @dlt.resource(
        name="CDEENT",
        table_name = TABLE_CLASSIFICATIONS["CDEENTBX"]["target_name"],
        write_disposition="merge",
        primary_key=PRIMARY_KEY_CONFIG["CDEENT"]["keys"],  # ← adapte avec ta vraie clé
    )
    def order_part_bx_header_resource():
        yield fetch_table(
            schema="BXNEGO_FIC",
            table_name="CDEENT",        
            date_column="datemodif",    
            days_back=days_back,
            load_mode=load_mode,
        )

    return order_part_bx_header_resource

@dlt.source
def db2_cdelig_source(
    load_mode: str = "incremental",
    days_back: int = 7,
):
    @dlt.resource(
        name="CDELIG",
        table_name = TABLE_CLASSIFICATIONS["CDELIG"]["target_name"],
        write_disposition="merge",
        primary_key=PRIMARY_KEY_CONFIG["CDELIG"]["keys"],  # ← adapte avec ta vraie clé
    )
    def order_part_line_resource():
        yield fetch_table(
            schema="BXNEGO_FIC",
            table_name="CDELIG",     
            days_back=days_back,
            load_mode=load_mode,
        )

    return order_part_line_resource

@dlt.source
def db2_cdelig_bx_source(
    load_mode: str = "full",
    days_back: int = 7,
):
    @dlt.resource(
        name="CDELIG",
        table_name = TABLE_CLASSIFICATIONS["CDELIGBX"]["target_name"],
        write_disposition="replace",
        primary_key=PRIMARY_KEY_CONFIG["CDELIG"]["keys"],  # ← adapte avec ta vraie clé
    )
    def order_part_line_bx_resource():
        for chunk in fetch_table_chunked(   # ← itère sur les chunks
            schema="BXNEGO_FIC",
            table_name="CDELIG",
            days_back=days_back,
            load_mode=load_mode,
    ):
            yield chunk 

    return order_part_line_bx_resource

@dlt.source
def db2_devent_source(
    load_mode: str = "incremental",
    days_back: int = 7,
):
    @dlt.resource(
        name="DEVENT",
        table_name = TABLE_CLASSIFICATIONS["DEVENT"]["target_name"],
        write_disposition="replace",
        primary_key=PRIMARY_KEY_CONFIG["DEVENT"]["keys"],
        columns=get_column_hints("DEVENT"), # Si besoin d'inférer un type de données
    )
    def quote_part_header_resource():
        yield fetch_table(
            schema="NEGOCE_FIC",
            table_name="DEVENT",        
            date_column="datecreat",    
            days_back=days_back,
            load_mode=load_mode,
        )

    return quote_part_header_resource

@dlt.source
def db2_devent_bx_source(
    load_mode: str = "full",
    days_back: int = 7,
):
    @dlt.resource(
        name="DEVENT",
        table_name = TABLE_CLASSIFICATIONS["DEVENTBX"]["target_name"],
        write_disposition="replace",
        primary_key=PRIMARY_KEY_CONFIG["DEVENT"]["keys"],  # ← adapte avec ta vraie clé
    )
    def quote_part_bx_header_resource():
        yield fetch_table(
            schema="BXNEGO_FIC",
            table_name="DEVENT",        
            date_column="datemodif",    
            days_back=days_back,
            load_mode=load_mode,
        )

    return quote_part_bx_header_resource


@dlt.source
def db2_facent_bx_source(
    load_mode: str = "incremental",
    days_back: int = 7,
):
    @dlt.resource(
        name="FACENT",
        table_name = TABLE_CLASSIFICATIONS["FACENTBX"]["target_name"],
        write_disposition="merge",
        primary_key=PRIMARY_KEY_CONFIG["FACENT"]["keys"],  # ← adapte avec ta vraie clé
    )
    def supply_bx_header_resource():
        yield fetch_table(
            schema="BXNEGO_FIC",
            table_name="FACENT",        
            date_column="datemodif",    
            days_back=days_back,
            load_mode=load_mode,
        )

    return supply_bx_header_resource

@dlt.source
def db2_tiers_source(
    load_mode: str = "full",
    days_back: int = 7,
):
    @dlt.resource(
        name="TIERS",
        table_name = TABLE_CLASSIFICATIONS["TIERS"]["target_name"],
        write_disposition="replace",
        primary_key=PRIMARY_KEY_CONFIG["TIERS"]["keys"],  # ← adapte avec ta vraie clé
    )
    def third_party_resource():
        yield fetch_table(
            schema="JADE_FIC",
            table_name="TIERS",        
            #date_column="datemodif",    
            days_back=days_back,
            load_mode=load_mode,
        )

    return third_party_resource

@dlt.source
def db2_jadirium_source(
    load_mode: str = "full",
    days_back: int = 7,
):
    @dlt.resource(
        name="JADIRIUM",
        table_name = TABLE_CLASSIFICATIONS["JADIRIUM"]["target_name"],
        write_disposition="replace",
        #primary_key=PRIMARY_KEY_CONFIG["JADIRIUM"]["keys"],  # ← adapte avec ta vraie clé
    )
    def link_jade_irum_resource():
        yield fetch_table(
            schema="JADE_FIC",
            table_name="JADIRIUM",        
            #date_column="datemodif",    
            days_back=days_back,
            load_mode=load_mode,
        )

    return link_jade_irum_resource

@dlt.source
def db2_faclig_bx_source(
    load_mode: str = "full",
    days_back: int = 7,
):
    @dlt.resource(
        name="CDELIG",
        table_name = TABLE_CLASSIFICATIONS["FACLIGBX"]["target_name"],
        write_disposition="replace",
        primary_key=PRIMARY_KEY_CONFIG["FACLIG"]["keys"],  # ← adapte avec ta vraie clé
    )
    def supply_bx_line_resource():
        for chunk in fetch_table_chunked(   # ← itère sur les chunks
            schema="BXNEGO_FIC",
            table_name="FACLIG",
            days_back=days_back,
            load_mode=load_mode,
    ):
            yield chunk 

    return supply_bx_line_resource


@dlt.source
def db2_livent_source(
    load_mode: str = "full",
    days_back: int = 7,
):
    @dlt.resource(
        name="LIVENT",
        table_name = TABLE_CLASSIFICATIONS["LIVENT"]["target_name"],
        write_disposition="replace",
        primary_key=PRIMARY_KEY_CONFIG["LIVENT"]["keys"],
        columns=get_column_hints("LIVENT"), # Si besoin d'inférer un type de données
    )
    def delivery_part_header_resource():
        yield fetch_table_chunked(
            schema="NEGOCE_FIC",
            table_name="LIVENT",        
            date_column="datecreat",    # On s'interesse qu'aux données créées après une certaine date
            days_back=days_back,
            load_mode=load_mode,
        )

    return delivery_part_header_resource

@dlt.source
def db2_livent_bx_source(
    load_mode: str = "full",
    days_back: int = 7,
):
    @dlt.resource(
        name="LIVENT",
        table_name = TABLE_CLASSIFICATIONS["LIVENTBX"]["target_name"],
        write_disposition="replace",
        primary_key=PRIMARY_KEY_CONFIG["LIVENT"]["keys"],  # ← adapte avec ta vraie clé
    )
    def delivery_part_bx_header_resource():
        yield fetch_table_chunked(
            schema="BXNEGO_FIC",
            table_name="LIVENT",        
            date_column="datemodif",    
            days_back=days_back,
            load_mode=load_mode,
        )

    return delivery_part_bx_header_resource

    # SQLAlchemy connection string pour DB2
#    conn_str = (
#        f"ibm_db_sa://{user}:{password}@{host}/"
#        f"?NAMING=1&LIBRARIES=NEGOCE_FIC"
#    )
#    #f"db2+ibm_db://{user}:{password}@{host}:{port}/"
#    return create_engine(conn_str)
    # Format connexion AS400 natif — pas de database name requis
#    conn = ibm_db.connect(
#        f"HOSTNAME={host};"
#        f"PORT=446;"
#        f"PROTOCOL=TCPIP;"
#        f"UID={user};"
#        f"PWD={password};"
#        f"NAMING=1;"
#        f"LIBRARIES=NEGOCE_FIC;",
#        "", ""
#    )
#    return ibm_db_dbi.Connection(conn)
    # jar_path = Path(__file__).parent.parent.parent.parent.parent / "jt400-20.0.7.jar"

    
    #@dlt.source
#def db2_cdeent_source(
#    load_mode: str = "incremental",
#    days_back: int = 7,
#):
#    """Source DLT pour la table order_part_header."""
#    engine = get_db2_engine()
#
#    yield sql_table(
#        credentials=engine,
#        table="CDEENT",
#        schema="NEGOCE_FIC",
#        incremental=dlt.sources.incremental(
#            "datemodif"       # colonne de date pour le filtre incrémental
#        ) if load_mode != "full" else None,
#    )