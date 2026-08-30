"""
DAG: bank_csv_read

Lee el archivo bank.csv (separado por ';') y reporta su contenido en el log.
No escribe en ninguna base de datos.

Requisitos previos:
  - El archivo bank.csv disponible en dags/data/bank.csv (carpeta montada).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task

# Ruta al CSV, relativa a este archivo de DAG (carpeta montada en el contenedor).
CSV_PATH = Path(__file__).parent / "data" / "bank.csv"


@dag(
    dag_id="bank_csv_read",
    description="Lee bank.csv y reporta su contenido en el log. No persiste nada.",
    schedule=None,            # Se ejecuta manualmente (trigger).
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bank", "csv", "curso-datos"],
)
def bank_csv_read():

    @task
    def read_csv() -> int:
        """Lee el CSV y deja en el log un resumen. Devuelve el número de filas."""
        if not CSV_PATH.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {CSV_PATH}")

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

        return len(df)

    read_csv()


bank_csv_read()
