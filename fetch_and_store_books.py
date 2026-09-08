"""Extrae libros desde la API de Open Library y los carga en Postgres.
 
Airflow 3.1 (Task SDK). Misma estructura de tasks que la versión Amazon:
create_table (SQLExecuteQueryOperator) + fetch (@task) + insert (@task).
 
OJO: el esquema de la tabla `books` cambió respecto de la versión anterior.
Si ya la creaste, `DROP TABLE books;` antes del primer run — `CREATE TABLE
IF NOT EXISTS` no altera una tabla existente y el INSERT va a fallar.
"""
 
from __future__ import annotations
 
import logging
import time
from datetime import timedelta
 
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
 
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
 
log = logging.getLogger(__name__)
 
CONN_ID = "postgres_bank"
SEARCH_URL = "https://openlibrary.org/search.json"
SEARCH_QUERY = "data engineering"
PAGE_SIZE = 50
MAX_PAGES = 10
FIELDS = (
    "key,title,author_name,first_publish_year,"
    "ratings_average,ratings_count,edition_count"
)
 
# Open Library pide un User-Agent identificable con contacto real.
# Reemplazá el mail por el tuyo antes de correrlo.
HEADERS = {
    "User-Agent": "airflow-diplomado-datos/1.0 (tu-mail@ejemplo.cl)",
    "Accept": "application/json",
}
 
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS books (
    id                 SERIAL PRIMARY KEY,
    work_key           TEXT NOT NULL,
    title              TEXT NOT NULL,
    authors            TEXT,
    first_publish_year INTEGER,
    rating             NUMERIC(4, 3),
    ratings_count      INTEGER,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT books_work_key_uniq UNIQUE (work_key)
);
"""
 
# Los ratings cambian en el tiempo, así que un DAG diario debe refrescarlos:
# DO UPDATE en vez de DO NOTHING.
UPSERT_SQL = """
INSERT INTO books (work_key, title, authors, first_publish_year, rating, ratings_count)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (work_key) DO UPDATE SET
    title              = EXCLUDED.title,
    authors            = EXCLUDED.authors,
    first_publish_year = EXCLUDED.first_publish_year,
    rating             = EXCLUDED.rating,
    ratings_count      = EXCLUDED.ratings_count,
    fetched_at         = now()
"""
 
 
def _http_session() -> requests.Session:
    """Session con backoff automático ante 429 y 5xx."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session
 
 
@dag(
    dag_id="fetch_and_store_books",
    description="Extrae libros desde Open Library y los almacena en Postgres",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["api", "postgres", "diplomado"],
)
def fetch_and_store_books():
 
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql=CREATE_TABLE_SQL,
    )
 
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def fetch_book_data(num_books: int = 50) -> list[dict]:
        books: list[dict] = []
        seen_keys: set[str] = set()
 
        with _http_session() as session:
            for page in range(1, MAX_PAGES + 1):
                if len(books) >= num_books:
                    break
 
                response = session.get(
                    SEARCH_URL,
                    params={
                        "q": SEARCH_QUERY,
                        "fields": FIELDS,
                        "limit": PAGE_SIZE,
                        "page": page,
                    },
                    timeout=30,
                )
                response.raise_for_status()
                payload = response.json()
 
                docs = payload.get("docs") or []
                if not docs:
                    log.info("Página %s vacía; fin de la paginación.", page)
                    break
 
                for doc in docs:
                    work_key = doc.get("key")
                    title = doc.get("title")
                    if not work_key or not title or work_key in seen_keys:
                        continue
 
                    seen_keys.add(work_key)
                    authors = doc.get("author_name") or []
                    books.append(
                        {
                            "work_key": work_key,
                            "title": title,
                            "authors": ", ".join(authors) or None,
                            "first_publish_year": doc.get("first_publish_year"),
                            "rating": doc.get("ratings_average"),
                            "ratings_count": doc.get("ratings_count"),
                        }
                    )
 
                log.info(
                    "Página %s: %s docs, %s libros acumulados (numFound=%s).",
                    page, len(docs), len(books), payload.get("numFound"),
                )
                time.sleep(1)  # cortesía con una API pública y gratuita
 
        if not books:
            raise ValueError(f"Open Library no devolvió libros para '{SEARCH_QUERY}'.")
 
        return books[:num_books]
 
    @task
    def insert_book_data(books: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        rows = [
            (
                b["work_key"],
                b["title"],
                b["authors"],
                b["first_publish_year"],
                b["rating"],
                b["ratings_count"],
            )
            for b in books
        ]
 
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, rows)
                affected = cur.rowcount
            conn.commit()
        finally:
            conn.close()
 
        log.info("Filas afectadas (insert + update): %s de %s.", affected, len(rows))
        return affected
 
    fetched = fetch_book_data(num_books=50)
    create_table >> insert_book_data(fetched)
 
 
fetch_and_store_books()
 