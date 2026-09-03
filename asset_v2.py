"""
Assets y constantes del pipeline v2.

Solo declaraciones: no contiene DAGs ni lógica.

La v2 repite el pipeline de la v1 (orchestrator.py) con una diferencia: los
resultados de los chequeos de calidad no quedan solo en el log, sino que se
guardan en dos tablas de PostgreSQL. Así se puede consultar el histórico de
calidad con SQL en vez de ir a leer los logs de Airflow.

Los Assets llevan sufijo '_v2' para que las dos versiones convivan en el mismo
entorno sin dispararse entre sí: el orquestador v1 mueve los Assets v1 y el v2
los suyos.
"""

from __future__ import annotations

from airflow.sdk import Asset

# --- Conexión ----------------------------------------------------------------
# Misma conexión que la v1: la base de destino viene del campo Database de la
# conexión 'postgres_bank' (en este entorno, la base 'airflow').
POSTGRES_CONN_ID = "postgres_bank"

# --- Tabla de datos ----------------------------------------------------------
# La carga del CSV va a su propia tabla para no pisar la que escribe la v1.
TABLE_NAME_V2 = "bank_v2"

# --- Tablas del reporte de calidad -------------------------------------------
# Modelo en dos niveles:
#   bank_quality_runs   → una fila por ejecución (el resumen).
#   bank_quality_checks → una fila por chequeo dentro de esa ejecución (el detalle).
QUALITY_RUNS_TABLE = "bank_quality_runs"
QUALITY_CHECKS_TABLE = "bank_quality_checks"

# --- Assets ------------------------------------------------------------------
BANK_CSV_RAW_V2 = Asset(name="bank_csv_raw_v2", group="bank_v2")
BANK_TABLE_LOADED_V2 = Asset(name="bank_table_loaded_v2", group="bank_v2")
BANK_QUALITY_REPORT_V2 = Asset(name="bank_quality_report_v2", group="bank_v2")
