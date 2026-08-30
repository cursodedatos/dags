"""
Asset del archivo CSV crudo.

Solo declaraciones: no contiene DAGs ni lógica.
"""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import Asset

# --- Ubicación del archivo ---------------------------------------------------
# El CSV vive en dags/data/bank.csv, junto a los DAGs.
CSV_PATH = Path(__file__).parent / "data" / "bank.csv"

# El CSV usa ';' como separador y comillas dobles en los textos.
CSV_SEP = ";"

# --- Asset -------------------------------------------------------------------
BANK_CSV_RAW = Asset(
    name="bank_csv_raw",
    uri=CSV_PATH.as_uri(),
    group="bank",
)
