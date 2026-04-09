"""
load_csv_to_bigquery.py
─────────────────────────────────────────────────────────────
Carga uno o varios archivos CSV a una tabla de BigQuery.

Uso básico:
    python scripts/load_csv_to_bigquery.py \
        --file data/orders.csv \
        --project my-gcp-project \
        --dataset olist \
        --table orders

Cargar todos los CSV de una carpeta:
    python scripts/load_csv_to_bigquery.py \
        --folder data/ \
        --project my-gcp-project \
        --dataset olist

Variables de entorno (alternativa a los flags):
    GCP_PROJECT   → ID del proyecto GCP
    BQ_DATASET    → Dataset destino en BigQuery
    GOOGLE_APPLICATION_CREDENTIALS → Ruta al JSON de la service account

Disposiciones de escritura (--write-mode):
    WRITE_TRUNCATE  → Reemplaza la tabla completa (default)
    WRITE_APPEND    → Agrega filas a la tabla existente
    WRITE_EMPTY     → Falla si la tabla ya tiene datos
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from tqdm import tqdm

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Cargar .env si existe ─────────────────────────────────────────────────────
load_dotenv()


# ── Funciones ─────────────────────────────────────────────────────────────────

def get_table_name(file_path: Path, override: str | None) -> str:
    """Deriva el nombre de tabla desde el nombre del archivo si no se especifica."""
    if override:
        return override
    # orders_2024.csv → orders_2024
    return file_path.stem.replace("-", "_")


def load_csv_to_bq(
    file_path: Path,
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    write_mode: str,
    delimiter: str,
    encoding: str,
) -> int:
    """
    Lee un CSV con pandas y lo sube a BigQuery.

    Retorna el número de filas cargadas.
    """
    log.info("Leyendo %s …", file_path)
    df = pd.read_csv(file_path, sep=delimiter, encoding=encoding, low_memory=False)
    row_count = len(df)
    log.info("  → %d filas, %d columnas", row_count, len(df.columns))

    table_ref = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,  # BigQuery infiere el esquema automáticamente
    )

    log.info("Subiendo a %s (modo: %s) …", table_ref, write_mode)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Espera a que termine el job

    log.info("  ✓ %d filas cargadas correctamente.", row_count)
    return row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carga archivos CSV a BigQuery.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Origen
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file", type=Path, help="Ruta a un único archivo CSV.")
    source.add_argument("--folder", type=Path, help="Carpeta con múltiples archivos CSV.")

    # Destino
    parser.add_argument(
        "--project",
        default=os.getenv("GCP_PROJECT"),
        help="ID del proyecto GCP. Por defecto: variable GCP_PROJECT.",
    )
    parser.add_argument(
        "--dataset",
        default=os.getenv("BQ_DATASET"),
        help="Dataset de BigQuery. Por defecto: variable BQ_DATASET.",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Nombre de la tabla destino. Si no se indica, se usa el nombre del archivo.",
    )

    # Opciones de carga
    parser.add_argument(
        "--write-mode",
        default="WRITE_TRUNCATE",
        choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
        help="Disposición de escritura en BigQuery (default: WRITE_TRUNCATE).",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="Delimitador del CSV (default: ',').",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding del archivo CSV (default: utf-8).",
    )

    return parser.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    # Validar credenciales mínimas
    if not args.project:
        log.error("Falta el proyecto GCP. Usa --project o define GCP_PROJECT en .env")
        sys.exit(1)
    if not args.dataset:
        log.error("Falta el dataset. Usa --dataset o define BQ_DATASET en .env")
        sys.exit(1)

    client = bigquery.Client(project=args.project)

    # Recolectar archivos a procesar
    if args.file:
        files = [args.file]
    else:
        files = sorted(args.folder.glob("*.csv"))
        if not files:
            log.error("No se encontraron archivos CSV en %s", args.folder)
            sys.exit(1)
        log.info("Se encontraron %d archivos CSV en %s", len(files), args.folder)

    total_rows = 0
    failed = []

    for csv_file in tqdm(files, desc="Cargando archivos", unit="archivo"):
        table_name = get_table_name(csv_file, args.table if len(files) == 1 else None)
        try:
            rows = load_csv_to_bq(
                file_path=csv_file,
                client=client,
                project=args.project,
                dataset=args.dataset,
                table=table_name,
                write_mode=args.write_mode,
                delimiter=args.delimiter,
                encoding=args.encoding,
            )
            total_rows += rows
        except Exception as exc:
            log.error("Error cargando %s: %s", csv_file.name, exc)
            failed.append(csv_file.name)

    # Resumen final
    log.info("─" * 50)
    log.info("Archivos procesados : %d", len(files) - len(failed))
    log.info("Filas cargadas      : %d", total_rows)
    if failed:
        log.warning("Archivos con error  : %s", ", ".join(failed))
        sys.exit(1)
    log.info("Carga completada exitosamente.")


if __name__ == "__main__":
    main()
