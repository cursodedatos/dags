"""
Definiciones compartidas para el pipeline de bank.csv coordinado con Assets.

Este módulo NO contiene ningún DAG: solo centraliza los Assets y las
constantes usadas por los DAGs productor y consumidores, de modo que todos
apunten exactamente al mismo Asset (los Assets se identifican por su nombre).
"""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import Asset

# --- Rutas y conexiones ------------------------------------------------------
# El CSV está montado en dags/data/bank.csv dentro del contenedor.
CSV_PATH = Path(__file__).parent / "data" / "bank.csv"

POSTGRES_CONN_ID = "postgres_bank"
TABLE_NAME = "bank"

# --- Assets (contratos de datos entre DAGs) ---------------------------------
# Asset del archivo crudo: lo publica el productor y lo consumen, en paralelo,
# la carga a Postgres y el proceso de calidad.
BANK_CSV_RAW = Asset(
    name="bank_csv_raw",
    uri=CSV_PATH.as_uri(),
    group="bank",
)

# Assets de salida, por si se quieren encadenar DAGs posteriores.
BANK_TABLE_LOADED = Asset(name="bank_table_loaded", group="bank")
BANK_QUALITY_REPORT = Asset(name="bank_quality_report", group="bank")
