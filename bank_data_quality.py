"""
DAG consumidor: bank_data_quality

Proceso de calidad de bank.csv, SEPARADO del DAG de carga y coordinado con
Assets. Se dispara cuando se actualiza el Asset 'bank_csv_raw' (lo publica
bank_source_publish) y corre EN PARALELO con bank_load_to_postgres, porque
ambos leen el CSV crudo y no dependen uno del otro.

Ejecuta un conjunto de chequeos de calidad sobre el archivo. Si algún chequeo
crítico falla, la tarea falla (y por tanto no se publica el Asset de reporte).
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.sdk import dag, task

from bank_common import BANK_CSV_RAW, BANK_QUALITY_REPORT, CSV_PATH

# Columnas que el dataset debe tener siempre.
EXPECTED_COLUMNS = [
    "age", "job", "marital", "education", "default", "balance", "housing",
    "loan", "contact", "day", "month", "duration", "campaign", "pdays",
    "previous", "poutcome", "y",
]

# Máximo porcentaje de nulos tolerado por columna.
MAX_NULL_RATIO = 0.05


@dag(
    dag_id="bank_data_quality",
    description="Proceso de calidad de bank.csv. Disparado por el Asset bank_csv_raw.",
    schedule=[BANK_CSV_RAW],   # Se ejecuta al actualizarse el Asset del CSV.
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bank", "assets", "calidad", "curso-datos"],
)
def bank_data_quality():

    @task(outlets=[BANK_QUALITY_REPORT])
    def run_quality_checks() -> dict:
        """Ejecuta los chequeos de calidad. Falla si alguno crítico no pasa."""
        df = pd.read_csv(CSV_PATH, sep=";")

        results: list[dict] = []

        def check(name: str, passed: bool, detail: str = "") -> None:
            results.append({"check": name, "passed": bool(passed), "detail": detail})

        # 1) Hay al menos una fila.
        n_rows = len(df)
        check("filas_no_vacio", n_rows > 0, f"{n_rows} filas")

        # 2) Están todas las columnas esperadas.
        missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
        check("columnas_esperadas", not missing, f"faltan: {missing}" if missing else "todas presentes")

        # 3) 'age' numérica y en rango razonable [18, 100].
        age_num = pd.to_numeric(df["age"], errors="coerce") if "age" in df else pd.Series(dtype=float)
        age_ok = age_num.notna().all() and age_num.between(18, 100).all()
        check("age_rango_valido", age_ok, "edades fuera de [18,100] o no numéricas" if not age_ok else "ok")

        # 4) 'balance' es numérica.
        bal_ok = "balance" in df and pd.to_numeric(df["balance"], errors="coerce").notna().all()
        check("balance_numerico", bal_ok, "hay balances no numéricos" if not bal_ok else "ok")

        # 5) 'y' solo contiene {yes, no}.
        y_vals = set(df["y"].dropna().unique()) if "y" in df else set()
        y_ok = y_vals.issubset({"yes", "no"})
        check("target_y_valido", y_ok, f"valores inesperados: {y_vals - {'yes', 'no'}}" if not y_ok else "ok")

        # 6) Sin filas duplicadas.
        n_dup = int(df.duplicated().sum())
        check("sin_duplicados", n_dup == 0, f"{n_dup} filas duplicadas")

        # 7) Porcentaje de nulos por columna bajo el umbral.
        null_ratio = (df.isna().mean()).to_dict()
        over = {c: round(r, 4) for c, r in null_ratio.items() if r > MAX_NULL_RATIO}
        check("nulos_bajo_umbral", not over, f"columnas sobre {MAX_NULL_RATIO:.0%}: {over}" if over else "ok")

        # --- Resumen ---------------------------------------------------------
        failed = [r for r in results if not r["passed"]]
        for r in results:
            estado = "OK  " if r["passed"] else "FALLA"
            print(f"[{estado}] {r['check']}: {r['detail']}")

        report = {
            "filas": n_rows,
            "total_checks": len(results),
            "fallidos": len(failed),
            "detalle": results,
        }

        if failed:
            nombres = ", ".join(r["check"] for r in failed)
            raise AirflowException(f"Chequeos de calidad fallidos: {nombres}")

        print(f"Todos los chequeos de calidad pasaron ({len(results)}/{len(results)}).")
        return report

    run_quality_checks()


bank_data_quality()
