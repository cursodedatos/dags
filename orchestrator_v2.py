"""
Orquestador del pipeline de bank.csv — versión 2.

Misma idea que orchestrator.py (v1): un orquestador manual publica el Asset del
CSV crudo y dos consumidores arrancan en paralelo, la carga y la calidad.

    orchestrator_v2 ──(bank_csv_raw_v2)──┬──▶ bank_load_to_postgres_v2
                                         │      (publica bank_table_loaded_v2)
                                         └──▶ bank_data_quality_v2
                                                (publica bank_quality_report_v2)

Lo nuevo de la v2: los resultados de los chequeos de calidad se GUARDAN EN LA
BASE DE DATOS, en dos tablas:

  bank_quality_runs    una fila por ejecución (resumen: filas, cuántos chequeos
                       pasaron, resultado PASS/FAIL, cuándo).
  bank_quality_checks  una fila por chequeo de esa ejecución (nombre, si pasó,
                       el detalle). Referencia a la ejecución por run_id.

El DAG de calidad se parte en tres tareas para que la escritura ocurra siempre,
incluso cuando la calidad falla:

  crear_tablas ──▶ ejecutar_chequeos ──▶ guardar_resultados

  · ejecutar_chequeos calcula el reporte y NO falla aunque haya chequeos rojos:
    devuelve el resultado por XCom.
  · guardar_resultados escribe en la base y RECIÉN AHÍ falla si hubo chequeos
    rojos. Si fallara antes de escribir, la base perdería justo el registro más
    interesante: el de la ejecución mala.

Como guardar_resultados falla cuando la calidad es mala, el Asset
bank_quality_report_v2 solo se publica en las ejecuciones sanas.

Las escrituras son idempotentes: antes de insertar se borra lo que hubiera de
ese mismo run_id, así un re-run no duplica filas.

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
from airflow.sdk import dag, get_current_context, task

from asset_csv import CSV_PATH, CSV_SEP
from asset_quality import AGE_MAX, AGE_MIN, EXPECTED_COLUMNS, MAX_NULL_RATIO
from asset_v2 import (
    BANK_CSV_RAW_V2,
    BANK_QUALITY_REPORT_V2,
    BANK_TABLE_LOADED_V2,
    POSTGRES_CONN_ID,
    QUALITY_CHECKS_TABLE,
    QUALITY_RUNS_TABLE,
    TABLE_NAME_V2,
)

FECHA_INICIO = datetime(2024, 1, 1)
TAGS = ["bank", "assets", "curso-datos", "v2"]


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
    dag_id="orchestrator_v2",
    description=(
        "Punto de entrada del pipeline v2: lee bank.csv y publica el Asset "
        "bank_csv_raw_v2, que dispara la carga y la calidad en paralelo."
    ),
    schedule=None,            # Se ejecuta manualmente (trigger).
    start_date=FECHA_INICIO,
    catchup=False,
    tags=[*TAGS, "csv", "orquestador"],
)
def orchestrator_v2():

    @task(outlets=[BANK_CSV_RAW_V2])
    def read_csv() -> int:
        """Lee el CSV, lo reporta y actualiza el Asset bank_csv_raw_v2."""
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
            f"\nAsset '{BANK_CSV_RAW_V2.name}' actualizado. Se disparan en "
            f"paralelo bank_load_to_postgres_v2 y bank_data_quality_v2."
        )
        return len(df)

    read_csv()


# =============================================================================
# DAG 2 — Consumidor: carga en PostgreSQL
# =============================================================================
@dag(
    dag_id="bank_load_to_postgres_v2",
    description=(
        "Carga bank.csv en la tabla bank_v2. Disparado por el Asset "
        "bank_csv_raw_v2."
    ),
    schedule=[BANK_CSV_RAW_V2],   # Se ejecuta al actualizarse el Asset del CSV.
    start_date=FECHA_INICIO,
    catchup=False,
    tags=[*TAGS, "postgres", "consumidor"],
)
def bank_load_to_postgres_v2():

    @task(outlets=[BANK_TABLE_LOADED_V2])
    def load_to_postgres() -> int:
        """Crea la tabla y carga el CSV. Devuelve las filas escritas."""
        df = _leer_csv()
        print(f"Leídas {len(df)} filas de {CSV_PATH}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        destino = hook.get_first(
            "SELECT current_database(), current_user, current_schema()"
        )
        print(
            f"Destino → base: {destino[0]} · usuario: {destino[1]} "
            f"· esquema: {destino[2]}"
        )

        # --- Creación de la tabla --------------------------------------------
        columnas_ddl = ",\n  ".join(
            f'"{col}" {_tipo_postgres(df[col])}' for col in df.columns
        )
        ddl = f'CREATE TABLE "{TABLE_NAME_V2}" (\n  {columnas_ddl}\n)'
        print(f"\nDDL:\n{ddl}")

        hook.run(f'DROP TABLE IF EXISTS "{TABLE_NAME_V2}"')
        hook.run(ddl)

        # --- Carga con COPY ---------------------------------------------------
        # Se serializa el DataFrame a CSV en memoria y se envía por STDIN.
        # En formato CSV, un campo vacío sin comillas es NULL para PostgreSQL.
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        columnas = ", ".join(f'"{col}"' for col in df.columns)
        copy_sql = f'COPY "{TABLE_NAME_V2}" ({columnas}) FROM STDIN WITH (FORMAT CSV)'

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

                cur.execute(f'SELECT count(*) FROM "{TABLE_NAME_V2}"')
                cargadas = cur.fetchone()[0]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(f"\nCargadas {cargadas} filas en la tabla '{TABLE_NAME_V2}'.")

        if cargadas != len(df):
            raise ValueError(
                f"Se leyeron {len(df)} filas pero la tabla quedó con {cargadas}."
            )

        return cargadas

    load_to_postgres()


# =============================================================================
# DAG 3 — Consumidor: chequeos de calidad, con resultados guardados en la base
# =============================================================================
@dag(
    dag_id="bank_data_quality_v2",
    description=(
        "Chequeos de calidad de bank.csv y persistencia del reporte en las "
        "tablas bank_quality_runs y bank_quality_checks."
    ),
    schedule=[BANK_CSV_RAW_V2],   # Mismo Asset que la carga: corren en paralelo.
    start_date=FECHA_INICIO,
    catchup=False,
    tags=[*TAGS, "calidad", "consumidor"],
)
def bank_data_quality_v2():

    @task
    def crear_tablas() -> None:
        """Crea las tablas del reporte si no existen (DDL idempotente)."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        ddl_runs = f"""
        CREATE TABLE IF NOT EXISTS "{QUALITY_RUNS_TABLE}" (
          run_id           TEXT PRIMARY KEY,
          dag_id           TEXT        NOT NULL,
          task_id          TEXT        NOT NULL,
          intento          INTEGER     NOT NULL,
          ejecutado_en     TIMESTAMPTZ NOT NULL DEFAULT now(),
          archivo          TEXT        NOT NULL,
          filas            BIGINT      NOT NULL,
          columnas         INTEGER     NOT NULL,
          total_checks     INTEGER     NOT NULL,
          checks_ok        INTEGER     NOT NULL,
          checks_fallidos  INTEGER     NOT NULL,
          resultado        TEXT        NOT NULL
        )
        """

        # run_id referencia a la ejecución; ON DELETE CASCADE hace que borrar el
        # resumen de un run se lleve también su detalle.
        ddl_checks = f"""
        CREATE TABLE IF NOT EXISTS "{QUALITY_CHECKS_TABLE}" (
          id          BIGSERIAL PRIMARY KEY,
          run_id      TEXT    NOT NULL
                      REFERENCES "{QUALITY_RUNS_TABLE}" (run_id) ON DELETE CASCADE,
          nro         INTEGER NOT NULL,
          check_name  TEXT    NOT NULL,
          passed      BOOLEAN NOT NULL,
          detail      TEXT
        )
        """

        idx_checks = (
            f'CREATE INDEX IF NOT EXISTS "{QUALITY_CHECKS_TABLE}_run_id_idx" '
            f'ON "{QUALITY_CHECKS_TABLE}" (run_id)'
        )

        for sentencia in (ddl_runs, ddl_checks, idx_checks):
            hook.run(sentencia)

        print(
            f"Tablas listas: '{QUALITY_RUNS_TABLE}' y '{QUALITY_CHECKS_TABLE}'."
        )

    @task
    def ejecutar_chequeos() -> dict:
        """Corre los chequeos y devuelve el reporte. No falla aunque haya rojos."""
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
        for r in resultados:
            estado = "OK   " if r["passed"] else "FALLA"
            print(f"[{estado}] {r['check']}: {r['detail']}")

        fallidos = [r for r in resultados if not r["passed"]]

        return {
            "archivo": str(CSV_PATH),
            "filas": n_filas,
            "columnas": len(df.columns),
            "total_checks": len(resultados),
            "checks_ok": len(resultados) - len(fallidos),
            "checks_fallidos": len(fallidos),
            "detalle": resultados,
        }

    @task(outlets=[BANK_QUALITY_REPORT_V2])
    def guardar_resultados(reporte: dict) -> dict:
        """Guarda el reporte en la base y falla si algún chequeo no pasó."""
        contexto = get_current_context()
        dag_run = contexto["dag_run"]
        ti = contexto["ti"]

        run_id = dag_run.run_id
        resultado = "FAIL" if reporte["checks_fallidos"] else "PASS"

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                # Idempotencia: un re-run del mismo run_id reemplaza su registro
                # en vez de duplicarlo (el CASCADE se lleva el detalle viejo).
                cur.execute(
                    f'DELETE FROM "{QUALITY_RUNS_TABLE}" WHERE run_id = %s',
                    (run_id,),
                )

                cur.execute(
                    f"""
                    INSERT INTO "{QUALITY_RUNS_TABLE}" (
                      run_id, dag_id, task_id, intento, archivo, filas, columnas,
                      total_checks, checks_ok, checks_fallidos, resultado
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        dag_run.dag_id,
                        ti.task_id,
                        ti.try_number,
                        reporte["archivo"],
                        reporte["filas"],
                        reporte["columnas"],
                        reporte["total_checks"],
                        reporte["checks_ok"],
                        reporte["checks_fallidos"],
                        resultado,
                    ),
                )

                filas_detalle = [
                    (run_id, nro, r["check"], r["passed"], r["detail"])
                    for nro, r in enumerate(reporte["detalle"], start=1)
                ]
                cur.executemany(
                    f"""
                    INSERT INTO "{QUALITY_CHECKS_TABLE}" (
                      run_id, nro, check_name, passed, detail
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    filas_detalle,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(
            f"Guardado en la base: 1 fila en '{QUALITY_RUNS_TABLE}' y "
            f"{len(reporte['detalle'])} en '{QUALITY_CHECKS_TABLE}' "
            f"(run_id={run_id}, resultado={resultado})."
        )
        print(
            f"Consulta: SELECT * FROM {QUALITY_CHECKS_TABLE} "
            f"WHERE run_id = '{run_id}' ORDER BY nro;"
        )

        if reporte["checks_fallidos"]:
            nombres = ", ".join(
                r["check"] for r in reporte["detalle"] if not r["passed"]
            )
            # El reporte ya quedó guardado: recién ahora se marca la falla, así
            # el Asset bank_quality_report_v2 no se publica.
            raise AirflowException(f"Chequeos de calidad fallidos: {nombres}")

        print(
            f"Todos los chequeos pasaron "
            f"({reporte['checks_ok']}/{reporte['total_checks']}). "
            f"Asset '{BANK_QUALITY_REPORT_V2.name}' actualizado."
        )
        return reporte

    crear_tablas() >> guardar_resultados(ejecutar_chequeos())


orchestrator_v2()
bank_load_to_postgres_v2()
bank_data_quality_v2()
