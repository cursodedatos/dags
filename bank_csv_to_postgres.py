"""
DAG: bank_csv_to_postgres

Lee el archivo bank.csv (separado por ';'), reporta su contenido en el log y
lo carga en la tabla 'bank' de PostgreSQL.

No usa Assets: se ejecuta manualmente con el botón de trigger.

La carga NO usa pandas.to_sql, que exige que la versión de SQLAlchemy instalada
alcance el mínimo que pide pandas (pandas >= 2.2 exige SQLAlchemy >= 2.0, y
Airflow fija SQLAlchemy < 2.0). En su lugar se crea la tabla con DDL explícito
y se carga con COPY vía psycopg2: no depende de SQLAlchemy y es más rápido.

La base de destino NO se define aquí: viene del campo Database de la conexión
'postgres_bank'. En este entorno esa conexión apunta a la base 'airflow', de
modo que la tabla queda como 'bank' dentro de ella.

Requisitos previos:
  - El archivo bank.csv disponible en dags/data/bank.csv
  - Conexión de Airflow 'postgres_bank' con Database = airflow.
"""

from __future__ import annotations

from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

# Ruta al CSV, relativa a este archivo de DAG.
CSV_PATH = Path(__file__).parent / "data" / "bank.csv"

POSTGRES_CONN_ID = "postgres_bank"
TABLE_NAME = "bank"


def _tipo_postgres(serie: pd.Series) -> str:
    """Traduce el dtype de pandas al tipo de columna de PostgreSQL."""
    if pd.api.types.is_bool_dtype(serie):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(serie):
        return "BIGINT"
    if pd.api.types.is_float_dtype(serie):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(serie):
        return "TIMESTAMP"
    return "TEXT"


@dag(
    dag_id="bank_csv_to_postgres",
    description="Lee bank.csv, lo reporta en el log y lo carga en PostgreSQL.",
    schedule=None,            # Se ejecuta manualmente (trigger).
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bank", "csv", "postgres", "curso-datos"],
)
def bank_csv_to_postgres():

    @task
    def read_and_load() -> int:
        """Lee el CSV, lo reporta en el log y lo escribe en PostgreSQL."""
        if not CSV_PATH.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {CSV_PATH}")

        # --- Lectura -------------------------------------------------------
        # El CSV usa ';' como separador y comillas dobles en los textos.
        df = pd.read_csv(CSV_PATH, sep=";")

        print(f"Archivo: {CSV_PATH}")
        print(f"Filas: {len(df)}  ·  Columnas: {len(df.columns)}")
        print(f"Columnas: {list(df.columns)}")
        print("\nTipos de dato:")
        print(df.dtypes.to_string())
        print("\nPrimeras 10 filas:")
        print(df.head(10).to_string(index=False))
        print("\nNulos por columna:")
        print(df.isna().sum().to_string())

        # --- Destino --------------------------------------------------------
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        destino = hook.get_first(
            "SELECT current_database(), current_user, current_schema()"
        )
        print(
            f"\nDestino → base: {destino[0]} · usuario: {destino[1]} "
            f"· esquema: {destino[2]}"
        )

        existe = hook.get_first(
            f"SELECT to_regclass('public.{TABLE_NAME}') IS NOT NULL"
        )[0]
        if existe:
            print(f"La tabla '{TABLE_NAME}' ya existe: se reemplaza.")
        else:
            print(f"La tabla '{TABLE_NAME}' no existe: se crea.")

        # --- Creación de la tabla -------------------------------------------
        columnas_ddl = ",\n  ".join(
            f'"{col}" {_tipo_postgres(df[col])}' for col in df.columns
        )
        ddl = f'CREATE TABLE "{TABLE_NAME}" (\n  {columnas_ddl}\n)'
        print(f"\nDDL:\n{ddl}")

        hook.run(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')
        hook.run(ddl)

        # --- Carga con COPY --------------------------------------------------
        # Se serializa el DataFrame a CSV en memoria y se envía por STDIN.
        # En formato CSV, un campo vacío sin comillas es NULL para PostgreSQL.
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        columnas = ", ".join(f'"{col}"' for col in df.columns)
        copy_sql = f'COPY "{TABLE_NAME}" ({columnas}) FROM STDIN WITH (FORMAT CSV)'

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.copy_expert(copy_sql, buffer)
                cur.execute(f'SELECT count(*) FROM "{TABLE_NAME}"')
                cargadas = cur.fetchone()[0]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(f"\nCargadas {cargadas} filas en la tabla '{TABLE_NAME}'.")

        if cargadas != len(df):
            raise ValueError(
                f"Se leyeron {len(df)} filas pero la tabla quedó con {cargadas}."
            )

        return cargadas

    read_and_load()


bank_csv_to_postgres()
