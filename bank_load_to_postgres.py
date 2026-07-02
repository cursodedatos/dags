"""
DAG consumidor: bank_load_to_postgres

Se dispara cuando se actualiza el Asset 'bank_csv_raw' (lo publica
bank_source_publish). Lee bank.csv y lo carga en la base PostgreSQL 'bank'.

Corre EN PARALELO con bank_data_quality: ambos consumen el mismo Asset del
CSV crudo y no dependen uno del otro.

Requisitos previos (ya configurados en este entorno):
  - Base de datos 'bank' creada en PostgreSQL.
  - Conexión de Airflow 'postgres_bank' apuntando a esa base.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from bank_common import (
    BANK_CSV_RAW,
    BANK_TABLE_LOADED,
    CSV_PATH,
    POSTGRES_CONN_ID,
    TABLE_NAME,
)


@dag(
    dag_id="bank_load_to_postgres",
    description="Carga bank.csv en PostgreSQL. Disparado por el Asset bank_csv_raw.",
    schedule=[BANK_CSV_RAW],   # Se ejecuta al actualizarse el Asset del CSV.
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bank", "assets", "carga", "postgres", "curso-datos"],
)
def bank_load_to_postgres():

    @task(outlets=[BANK_TABLE_LOADED])
    def load_csv_to_postgres() -> int:
        """Lee el CSV y lo escribe en PostgreSQL. Devuelve el número de filas."""
        # El CSV usa ';' como separador y comillas dobles en los textos.
        df = pd.read_csv(CSV_PATH, sep=";")

        engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()

        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists="replace",   # Reemplaza la tabla en cada ejecución.
            index=False,
            chunksize=1000,
            method="multi",
        )

        print(f"Cargadas {len(df)} filas en la tabla '{TABLE_NAME}'.")
        return len(df)

    load_csv_to_postgres()


bank_load_to_postgres()
