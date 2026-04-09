"""
setup_bigquery.py
─────────────────────────────────────────────────────────────
Crea los datasets y tablas operativas en BigQuery.
Ejecutar UNA SOLA VEZ antes de correr cualquier pipeline.

Crea:
  - raw      → datos en bruto desde los CSV
  - curated  → datos transformados y limpios
  - ops      → logs de ejecución, métricas y calidad

Uso:
    python scripts/setup_bigquery.py --project my-gcp-project
"""

import argparse
import logging
import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import Conflict

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Schemas de las tablas operativas (ops) ────────────────────────────────────

OPS_SCHEMAS = {
    "batch_logs": [
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED", description="Identificador único de ejecución"),
        bigquery.SchemaField("batch_type", "STRING", description="FULL o INCREMENTAL"),
        bigquery.SchemaField("status", "STRING", description="RUNNING / SUCCESS / FAILED"),
        bigquery.SchemaField("started_at", "TIMESTAMP", description="Inicio de la ejecución"),
        bigquery.SchemaField("finished_at", "TIMESTAMP", description="Fin de la ejecución"),
        bigquery.SchemaField("watermark_from", "TIMESTAMP", description="Límite inferior del rango de datos procesados"),
        bigquery.SchemaField("watermark_to", "TIMESTAMP", description="Límite superior del rango de datos procesados"),
        bigquery.SchemaField("tables_processed", "INTEGER", description="Cantidad de tablas procesadas"),
        bigquery.SchemaField("total_rows_loaded", "INTEGER", description="Total de filas cargadas en curated"),
        bigquery.SchemaField("error_message", "STRING", description="Mensaje de error si status=FAILED"),
    ],
    "table_metrics": [
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED", description="Nombre de la tabla procesada"),
        bigquery.SchemaField("source_file", "STRING", description="Archivo CSV de origen"),
        bigquery.SchemaField("raw_row_count", "INTEGER", description="Filas en raw antes del filtro"),
        bigquery.SchemaField("loaded_row_count", "INTEGER", description="Filas cargadas en curated"),
        bigquery.SchemaField("rejected_row_count", "INTEGER", description="Filas rechazadas (nulos en PKs, duplicados)"),
        bigquery.SchemaField("new_row_count", "INTEGER", description="Filas nuevas (solo incremental)"),
        bigquery.SchemaField("updated_row_count", "INTEGER", description="Filas actualizadas (solo incremental)"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ],
    "quality_checks": [
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("check_name", "STRING", mode="REQUIRED", description="Nombre del control de calidad"),
        bigquery.SchemaField("table_name", "STRING", description="Tabla sobre la que aplica el control"),
        bigquery.SchemaField("severity", "STRING", description="ERROR / WARNING / INFO"),
        bigquery.SchemaField("total_checked", "INTEGER", description="Total de registros evaluados"),
        bigquery.SchemaField("failed_count", "INTEGER", description="Registros que fallaron el control"),
        bigquery.SchemaField("pass_rate", "FLOAT", description="Tasa de registros que pasan (0.0 a 1.0)"),
        bigquery.SchemaField("status", "STRING", description="PASS / FAIL / WARNING"),
        bigquery.SchemaField("details", "STRING", description="Descripción adicional del resultado"),
        bigquery.SchemaField("checked_at", "TIMESTAMP"),
    ],
}


def create_dataset(client: bigquery.Client, project: str, dataset_id: str, location: str) -> None:
    dataset_ref = f"{project}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    dataset.description = {
        "raw": "Datos cargados tal como vienen del origen (CSV Olist). No modificar.",
        "curated": "Modelo de datos limpio con transformaciones y campos de trazabilidad.",
        "ops": "Tablas operativas: logs de ejecución, métricas y controles de calidad.",
    }.get(dataset_id, "")

    try:
        client.create_dataset(dataset)
        log.info("  ✓ Dataset creado: %s", dataset_ref)
    except Conflict:
        log.info("  · Dataset ya existe: %s (sin cambios)", dataset_ref)


def create_ops_table(client: bigquery.Client, project: str, table_name: str, schema: list) -> None:
    table_ref = f"{project}.ops.{table_name}"
    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.create_table(table)
        log.info("  ✓ Tabla creada: %s", table_ref)
    except Conflict:
        log.info("  · Tabla ya existe: %s (sin cambios)", table_ref)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Setup inicial de BigQuery para el pipeline Olist.")
    parser.add_argument(
        "--project",
        default=os.getenv("GCP_PROJECT"),
        help="ID del proyecto GCP. Por defecto: variable GCP_PROJECT.",
    )
    parser.add_argument(
        "--location",
        default=os.getenv("BQ_LOCATION", "US"),
        help="Ubicación de los datasets (default: US).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.project:
        raise ValueError("Falta el proyecto GCP. Usa --project o define GCP_PROJECT en .env")

    client = bigquery.Client(project=args.project)

    log.info("=== Creando datasets ===")
    for ds in ["raw", "curated", "ops"]:
        create_dataset(client, args.project, ds, args.location)

    log.info("=== Creando tablas ops ===")
    for table_name, schema in OPS_SCHEMAS.items():
        create_ops_table(client, args.project, table_name, schema)

    log.info("=== Setup completado. ===")
    log.info("Próximo paso: python scripts/load_raw.py --project %s", args.project)


if __name__ == "__main__":
    main()
