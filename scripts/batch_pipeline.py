"""
batch_pipeline.py  —  Parte C del assessment
─────────────────────────────────────────────────────────────
Pipeline batch COMPLETO (carga inicial / full refresh).

Flujo:
  1. Genera un batch_id único (UUID)
  2. Registra el inicio en ops.batch_logs
  3. Por cada tabla:
       a. Ejecuta el SQL de transformación (sql/curated/<tabla>.sql)
       b. Escribe el resultado en curated.<tabla> (WRITE_TRUNCATE)
       c. Registra métricas en ops.table_metrics
  4. Registra el fin en ops.batch_logs (SUCCESS o FAILED)

Uso:
    python scripts/batch_pipeline.py --project my-gcp-project

Variables de entorno (.env):
    GCP_PROJECT   → ID del proyecto GCP
"""

import argparse
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent.parent / "sql" / "curated"

# Orden de carga (respetar dependencias entre tablas)
TABLES = [
    "customers",
    "sellers",
    "products",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
]


def get_row_count(client: bigquery.Client, table_ref: str) -> int:
    result = client.query(f"SELECT COUNT(*) as n FROM `{table_ref}`").result()
    return next(result).n


def load_sql(table: str, project: str) -> str:
    """Lee el SQL de sql/curated/<table>.sql y sustituye el placeholder de proyecto."""
    sql_path = SQL_DIR / f"{table}.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"No se encontró el SQL: {sql_path}")
    return sql_path.read_text().replace("{project}", project)


def run_table(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    table: str,
) -> dict:
    """
    Ejecuta la transformación de una tabla y la carga en curated.
    Retorna métricas: raw_count, loaded_count, rejected_count.
    """
    log.info("── Procesando tabla: %s ──", table)

    raw_ref = f"{project}.raw.{table}"
    curated_ref = f"{project}.curated.{table}"

    # Conteo en raw antes de transformar
    raw_count = get_row_count(client, raw_ref)
    log.info("  raw.%s: %d filas", table, raw_count)

    # Leer y parametrizar el SQL de transformación
    sql = load_sql(table, project)
    sql_with_batch = sql.replace("@batch_id", f"'{batch_id}'")

    # Ejecutar la query y escribir en curated
    job_config = bigquery.QueryJobConfig(
        destination=curated_ref,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    log.info("  Ejecutando transformación SQL …")
    job = client.query(sql_with_batch, job_config=job_config)
    job.result()

    loaded_count = get_row_count(client, curated_ref)
    rejected_count = raw_count - loaded_count
    log.info("  ✓ curated.%s: %d filas (%d rechazadas)", table, loaded_count, rejected_count)

    return {
        "table_name": table,
        "raw_count": raw_count,
        "loaded_count": loaded_count,
        "rejected_count": rejected_count,
    }


def write_batch_log(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    tables_processed: int,
    total_rows: int,
    error_message: str = None,
) -> None:
    table_ref = f"{project}.ops.batch_logs"
    row = {
        "batch_id": batch_id,
        "batch_type": "FULL",
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "watermark_from": None,
        "watermark_to": None,
        "tables_processed": tables_processed,
        "total_rows_loaded": total_rows,
        "error_message": error_message,
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        log.warning("Error escribiendo batch_logs: %s", errors)


def write_table_metrics(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    metrics_list: list,
) -> None:
    table_ref = f"{project}.ops.table_metrics"
    rows = [
        {
            "batch_id": batch_id,
            "table_name": m["table_name"],
            "source_file": f"olist_{m['table_name']}_dataset.csv",
            "raw_row_count": m["raw_count"],
            "loaded_row_count": m["loaded_count"],
            "rejected_row_count": m["rejected_count"],
            "new_row_count": None,
            "updated_row_count": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        for m in metrics_list
    ]
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        log.warning("Error escribiendo table_metrics: %s", errors)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch pipeline completo: raw → curated.")
    parser.add_argument(
        "--project",
        default=os.getenv("GCP_PROJECT"),
        help="ID del proyecto GCP.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.project:
        raise ValueError("Falta el proyecto GCP. Usa --project o define GCP_PROJECT en .env")

    client = bigquery.Client(project=args.project)
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    log.info("=" * 55)
    log.info("BATCH FULL   batch_id=%s", batch_id)
    log.info("Inicio: %s", started_at.strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("=" * 55)

    metrics_list = []
    error_msg = None
    status = "SUCCESS"

    try:
        for table in TABLES:
            metrics = run_table(client, args.project, batch_id, table)
            metrics_list.append(metrics)

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        log.error("Pipeline fallido: %s", exc)

    finally:
        finished_at = datetime.now(timezone.utc)
        total_rows = sum(m["loaded_count"] for m in metrics_list)

        if metrics_list:
            write_table_metrics(client, args.project, batch_id, metrics_list)

        write_batch_log(
            client=client,
            project=args.project,
            batch_id=batch_id,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            tables_processed=len(metrics_list),
            total_rows=total_rows,
            error_message=error_msg,
        )

    duration = (finished_at - started_at).total_seconds()
    log.info("=" * 55)
    log.info("BATCH FINALIZADO")
    log.info("Status  : %s", status)
    log.info("Tablas  : %d procesadas", len(metrics_list))
    log.info("Filas   : %d cargadas en curated", total_rows)
    log.info("Duración: %.1f segundos", duration)
    log.info("=" * 55)

    if status == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
