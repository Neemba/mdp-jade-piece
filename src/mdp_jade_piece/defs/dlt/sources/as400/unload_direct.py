"""
AS400 ODBC direct load to Snowflake (v2 — Parquet pipeline).

Re-exported from mdp_common — all logic (ODBC connection, Snowflake auth,
PUT/COPY INTO) is now in the shared library.
"""
from mdp_common.db2.unload_direct_v2 import extract_and_stage_to_snowflake

__all__ = ["extract_and_stage_to_snowflake"]
