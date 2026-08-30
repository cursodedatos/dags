"""
Orquestador del pipeline de bank.csv.

Los Assets y las constantes se declaran en asset_csv.py, asset_postgres.py y
asset_quality.py. Aquí vive toda la lógica y los DAGs que la ejecutan.

El encadenamiento no se hace con dependencias entre tareas ni con
TriggerDagRunOperator, sino con Assets:

                                    ┌──▶ bank_load_to_postgres
                                    │      (publica bank_table_loaded)
    orchestrator ──(bank_csv_raw)───┤
                                    │
                                    └──▶ bank_data_quality
                                           (publica bank_quality_report)

'orchestrator' se dispara a mano. Al terminar bien actualiza el Asset
'bank_csv_raw', y los dos consumidores arrancan EN PARALELO: ambos leen el CSV
crudo y no dependen uno del otro.

Como corren en paralelo, un fallo de calidad NO impide la carga. Si se quisiera
que la calidad fuera una compuerta previa, bank_load_to_postgres debería
consumir BANK_QUALITY_REPORT en vez de BANK_CSV_RAW.

La carga NO usa pandas.to_sql, que exige que la versión de SQLAlchemy instalada
alcance el mínimo que pide pandas (pandas >= 2.2 exige SQLAlchemy >= 2.0, y
Airflow fija SQLAlchemy < 2.0). En su lugar se crea la tabla con DDL explícito
y se carga con COPY vía psycopg2: no depende de SQLAlchemy y es más rápido.

Requisitos previos:
  - El archivo bank.csv disponible en dags/data/bank.csv
  - Conexión de Airflow 'postgres_bank' con Database = airflow.
"""

from __future__ import annotations

from datetime import datetime
from io import StringIO

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from asset_csv import BANK_CSV_RAW, CSV_PATH, CSV_SEP
from asset_postgres import BANK_TABLE_LOADED, POSTGRES_CONN_ID, TABLE_NAME
from asset_quality import (
    AGE_MAX,
    AGE_MIN,
    BANK_QUALITY_REPORT,
    EXPECTED_COLUMNS,
    MAX_NULL_RATIO,
)

FECHA_INICIO = datetime(2024, 1, 1)
TAGS = ["bank", "assets", "curso-datos"]


def _leer_csv() -> pd.DataFrame:
    """Lee el CSV y devuelve el DataFrame. Falla si no existe o viene vacío."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, sep=CSV_SEP)

    if df.empty:
        raise ValueError(f"El archivo {CSV_PATH} no tiene filas.")

    return df


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


# =============================================================================
# DAG 1 — Orquestador: lee el CSV y publica el Asset del archivo crudo
# =============================================================================
@dag(
    dag_id="orchestrator",
    description=(
        "Punto de entrada del pipeline: lee bank.csv, lo reporta y publica el "
        "Asset bank_csv_raw, que dispara la carga y la calidad en paralelo."
    ),
    schedule=None,            # Se ejecuta manualmente (trigger).
    start_date=FECHA_INICIO,
    catchup=False,
    tags=[*TAGS, "csv", "orquestador"],
)
def orchestrator():

    @task(outlets=[BANK_CSV_RAW])
    def read_csv() -> int:
        """Lee el CSV, lo reporta y actualiza el Asset bank_csv_raw."""
        df = _leer_csv()

        print(f"Archivo: {CSV_PATH}")
        print(f"Filas: {len(df)}  ·  Columnas: {len(df.columns)}")
        print(f"Columnas: {list(df.columns)}")
        print("\nTipos de dato:")
        print(df.dtypes.to_string())
        print("\nPrimeras 10 filas:")
        print(df.head(10).to_string(index=False))
        print("\nNulos por columna:")
        print(df.isna().sum().to_string())

        print(
            f"\nAsset '{BANK_CSV_RAW.name}' actualizado. Se disparan en paralelo "
            f"bank_load_to_postgres y bank_data_quality."
        )
        return len(df)

    read_csv()


# =============================================================================
# DAG 2 — Consumidor: carga en PostgreSQL
# =============================================================================
@dag(
    dag_id="bank_load_to_postgres",
    description="Carga bank.csv en PostgreSQL. Disparado por el Asset bank_csv_raw.",
    schedule=[BANK_CSV_RAW],   # Se ejecuta al actualizarse el Asset del CSV.
    start_date=FECHA_INICIO,
    catchup=False,
    tags=[*TAGS, "postgres", "consumidor"],
)
def bank_load_to_postgres():

    @task(outlets=[BANK_TABLE_LOADED])
    def load_to_postgres() -> int:
        """Crea la tabla y carga el CSV. Devuelve las filas escritas."""
        df = _leer_csv()
        print(f"Leídas {len(df)} filas de {CSV_PATH}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Deja constancia en el log de dónde se está escribiendo realmente.
        destino = hook.get_first(
            "SELECT current_database(), current_user, current_schema()"
        )
        print(
            f"Destino → base: {destino[0]} · usuario: {destino[1]} "
            f"· esquema: {destino[2]}"
        )

        existe = hook.get_first(
            f"SELECT to_regclass('public.{TABLE_NAME}') IS NOT NULL"
        )[0]
        if existe:
            print(f"La tabla '{TABLE_NAME}' ya existe: se reemplaza.")
        else:
            print(f"La tabla '{TABLE_NAME}' no existe: se crea.")

        # --- Creación de la tabla ------------------------------------------
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
                # El provider de Postgres usa psycopg2 en unas versiones y
                # psycopg 3 en otras, y la API de COPY cambia entre ambas.
                if hasattr(cur, "copy_expert"):
                    cur.copy_expert(copy_sql, buffer)          # psycopg2
                else:
                    with cur.copy(copy_sql) as copy:           # psycopg 3
                        copy.write(buffer.read())

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

    load_to_postgres()


# =============================================================================
# DAG 3 — Consumidor: chequeos de calidad, en paralelo con la carga
# =============================================================================
@dag(
    dag_id="bank_data_quality",
    description="Chequeos de calidad de bank.csv. Disparado por el Asset bank_csv_raw.",
    schedule=[BANK_CSV_RAW],   # Mismo Asset que la carga: corren en paralelo.
    start_date=FECHA_INICIO,
    catchup=False,
    tags=[*TAGS, "calidad", "consumidor"],
)
def bank_data_quality():

    @task(outlets=[BANK_QUALITY_REPORT])
    def run_quality_checks() -> dict:
        """Ejecuta los chequeos de calidad. Falla si alguno no pasa."""
        df = _leer_csv()

        resultados: list[dict] = []

        def check(nombre: str, paso: bool, detalle: str = "") -> None:
            resultados.append(
                {"check": nombre, "passed": bool(paso), "detail": detalle}
            )

        # 1) Hay al menos una fila.
        n_filas = len(df)
        check("filas_no_vacio", n_filas > 0, f"{n_filas} filas")

        # 2) Están todas las columnas esperadas.
        faltantes = [c for c in EXPECTED_COLUMNS if c not in df.columns]
        check(
            "columnas_esperadas",
            not faltantes,
            f"faltan: {faltantes}" if faltantes else "todas presentes",
        )

        # 3) 'age' numérica y en rango razonable.
        age = (
            pd.to_numeric(df["age"], errors="coerce")
            if "age" in df
            else pd.Series(dtype=float)
        )
        age_ok = age.notna().all() and age.between(AGE_MIN, AGE_MAX).all()
        check(
            "age_rango_valido",
            age_ok,
            "ok" if age_ok else f"edades fuera de [{AGE_MIN},{AGE_MAX}] o no numéricas",
        )

        # 4) 'balance' es numérica.
        balance_ok = (
            "balance" in df
            and pd.to_numeric(df["balance"], errors="coerce").notna().all()
        )
        check(
            "balance_numerico",
            balance_ok,
            "ok" if balance_ok else "hay balances no numéricos",
        )

        # 5) 'y' solo contiene {yes, no}.
        valores_y = set(df["y"].dropna().unique()) if "y" in df else set()
        y_ok = valores_y.issubset({"yes", "no"})
        check(
            "target_y_valido",
            y_ok,
            "ok" if y_ok else f"valores inesperados: {valores_y - {'yes', 'no'}}",
        )

        # 6) Sin filas duplicadas.
        n_dup = int(df.duplicated().sum())
        check("sin_duplicados", n_dup == 0, f"{n_dup} filas duplicadas")

        # 7) Porcentaje de nulos por columna bajo el umbral.
        ratio_nulos = df.isna().mean().to_dict()
        sobre = {c: round(r, 4) for c, r in ratio_nulos.items() if r > MAX_NULL_RATIO}
        check(
            "nulos_bajo_umbral",
            not sobre,
            "ok" if not sobre else f"columnas sobre {MAX_NULL_RATIO:.0%}: {sobre}",
        )

        # --- Resumen ---------------------------------------------------------
        fallidos = [r for r in resultados if not r["passed"]]
        for r in resultados:
            estado = "OK   " if r["passed"] else "FALLA"
            print(f"[{estado}] {r['check']}: {r['detail']}")

        reporte = {
            "filas": n_filas,
            "total_checks": len(resultados),
            "fallidos": len(fallidos),
            "detalle": resultados,
        }

        if fallidos:
            nombres = ", ".join(r["check"] for r in fallidos)
            raise AirflowException(f"Chequeos de calidad fallidos: {nombres}")

        print(
            f"\nTodos los chequeos pasaron ({len(resultados)}/{len(resultados)}). "
            f"Asset '{BANK_QUALITY_REPORT.name}' actualizado."
        )
        return reporte

    run_quality_checks()


orchestrator()
bank_load_to_postgres()
bank_data_quality()
