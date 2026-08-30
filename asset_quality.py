"""
Asset del reporte de calidad de bank.csv.

Solo declaraciones: no contiene DAGs ni lógica.
"""

from __future__ import annotations

from airflow.sdk import Asset

# --- Parámetros de los chequeos ----------------------------------------------
# Columnas que el dataset debe tener siempre.
EXPECTED_COLUMNS = [
    "age", "job", "marital", "education", "default", "balance", "housing",
    "loan", "contact", "day", "month", "duration", "campaign", "pdays",
    "previous", "poutcome", "y",
]

# Máximo porcentaje de nulos tolerado por columna.
MAX_NULL_RATIO = 0.05

# Rango razonable para la edad.
AGE_MIN = 18
AGE_MAX = 100

# --- Asset -------------------------------------------------------------------
# Sin uri: el reporte no se materializa como archivo, y el nombre basta para
# identificarlo entre DAGs.
BANK_QUALITY_REPORT = Asset(
    name="bank_quality_report",
    group="bank",
)
