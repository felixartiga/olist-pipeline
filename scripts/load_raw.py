"""
load_raw.py
─────────────────────────────────────────────────────────────
Carga todos los CSVs del dataset Olist al dataset `raw` en BigQuery.
Los datos se cargan TAL COMO VIENEN del origen, sin transformaciones.

Uso:
    python scripts/load_raw.py --project my-gcp-project

Variables de entorno (.env):
    GCP_PROJECT  → ID del proyecto GCP
    DATA_DIR     → Carpeta con los CSVs (default: data/)
"""

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Mapeo: nombre de archivo CSV → nombre de tabla en raw
CSV_TABLE_MAP = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_translation",
}

# Columnas que deben leerse como string para preservar valores originales
# (evita que pandas convierta IDs numéricos o zip codes)
STRING_COLUMNS = {
    "olist_orders_dataset.csv": ["order_id", "customer_id"],
    "olist_order_items_dataset.csv": ["order_id", "product_id", "seller_id"],
    "olist_order_payments_dataset.csv": ["order_id"],
    "olist_order_reviews_dataset.csv": ["review_id", "order_id"],
    "olist_customers_dataset.csv": ["customer_id", "customer_unique_id", "customer_zip_code_prefix"],
    "olist_products_dataset.csv": ["product_id"],
    "olist_sellers_dataset.csv": ["seller_id", "seller_zip_code_prefix"],
    "olist_geolocation_dataset.csv": ["geolocation_zip_code_prefix"],
    "product_category_name_translation.csv": [],
}


def load_csv_to_raw(
    file_path: Path,
    table_name: str,
    client: bigquery.Client,
    project: str,
) -> int:
    """Carga un CSV al dataset raw. Retorna cantidad de filas cargadas."""
    filename = file_path.name
    dtype_map = {col: str for col in STRING_COLUMNS.get(filename, [])}

    log.info("Leyendo %s …", filename)
    df = pd.read_csv(file_path, dtype=dtype_map, low_memory=False)
    row_count = len(df)
    log.info("  → %d filas, %d columnas", row_count, len(df.columns))

    table_ref = f"{project}.raw.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    log.info("  → Subiendo a %s …", table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    log.info("  ✓ %d filas cargadas en %s", row_count, table_ref)
    return row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga CSVs Olist al dataset raw de BigQuery.")
    parser.add_argument(
        "--project",
        default=os.getenv("GCP_PROJECT"),
        help="ID del proyecto GCP.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.getenv("DATA_DIR", "data")),
        help="Carpeta con los CSVs (default: data/).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.project:
        raise ValueError("Falta el proyecto GCP. Usa --project o define GCP_PROJECT en .env")

    if not args.data_dir.exists():
        raise FileNotFoundError(f"La carpeta de datos no existe: {args.data_dir}")

    client = bigquery.Client(project=args.project)

    log.info("=== Carga inicial a dataset raw ===")
    log.info("Proyecto : %s", args.project)
    log.info("Carpeta  : %s", args.data_dir.resolve())

    total_rows = 0
    failed = []

    for filename, table_name in tqdm(CSV_TABLE_MAP.items(), desc="Cargando a raw", unit="tabla"):
        file_path = args.data_dir / filename
        if not file_path.exists():
            log.warning("  ⚠ Archivo no encontrado, saltando: %s", filename)
            continue
        try:
            rows = load_csv_to_raw(file_path, table_name, client, args.project)
            total_rows += rows
        except Exception as exc:
            log.error("  ✗ Error cargando %s: %s", filename, exc)
            failed.append(filename)

    log.info("─" * 50)
    log.info("Tablas cargadas : %d / %d", len(CSV_TABLE_MAP) - len(failed), len(CSV_TABLE_MAP))
    log.info("Filas totales   : %d", total_rows)
    if failed:
        log.warning("Con errores     : %s", ", ".join(failed))
    else:
        log.info("✓ Carga raw completada exitosamente.")
    log.info("Próximo paso: python scripts/batch_pipeline.py --project %s", args.project)


if __name__ == "__main__":
    main()
