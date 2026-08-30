"""
Asset de la tabla en PostgreSQL.

Solo declaraciones: no contiene DAGs ni lógica.
"""

from __future__ import annotations

from airflow.sdk import Asset

# --- Conexión y destino ------------------------------------------------------
# La base de destino NO se define aquí: viene del campo Database de la
# conexión 'postgres_bank'. En este entorno apunta a la base 'airflow'.
POSTGRES_CONN_ID = "postgres_bank"
TABLE_NAME = "bank"

# --- Asset -------------------------------------------------------------------
# Sin uri: una tabla no tiene una ruta de archivo, y el nombre basta para
# identificarla entre DAGs.
BANK_TABLE_LOADED = Asset(
    name="bank_table_loaded",
    group="bank",
)
