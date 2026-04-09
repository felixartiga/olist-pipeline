"""
batch_incremental.py  —  Parte D del assessment
─────────────────────────────────────────────────────────────
Pipeline batch INCREMENTAL (carga delta).

Estrategia incremental documentada:
  - Clave incremental: order_purchase_timestamp en la tabla orders.
  - Watermark: se obtiene el MAX(order_purchase_timestamp) de la
    última ejecución exitosa registrada en ops.batch_logs.
  - En la primera ejecución incremental, se usa un CUTOFF_DATE por
    defecto (2018-07-01) que divide el dataset aproximadamente en
    85% (batch full) vs 15% (delta).
  - Para tablas de referencia (customers, sellers, products):
    se procesan solo los registros relacionados con órdenes nuevas.
  - Lógica UPSERT: BigQuery MERGE inserta nuevos y actualiza existentes.
  - Registros nuevos vs actualizados se identifican comparando
    order_status entre raw y curated.

Simulación de delta:
  - El batch full carga TODOS los datos.
  - El incremental filtra órdenes con order_purchase_timestamp
    posterior al watermark del último batch exitoso.
  - Para simular "actualizaciones", se procesan algunas órdenes
    ya existentes cuyo order_status en raw difiera del curated.

Uso:
    python scripts/batch_incremental.py --project my-gcp-project
    python scripts/batch_incremental.py --project my-gcp-project --cutoff 2018-07-01
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

# Cutoff por defecto si no hay batch previo
DEFAULT_CUTOFF = "2018-07-01 00:00:00"


def get_last_watermark(client: bigquery.Client, project: str) -> str | None:
    """Obtiene el watermark_to del último batch exitoso. Retorna None si no hay."""
    sql = f"""
        SELECT watermark_to
        FROM `{project}.ops.batch_logs`
        WHERE status = 'SUCCESS' AND watermark_to IS NOT NULL
        ORDER BY finished_at DESC
        LIMIT 1
    """
    try:
        results = list(client.query(sql).result())
        if results and results[0].watermark_to:
            return results[0].watermark_to.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return None


def get_max_timestamp(client: bigquery.Client, project: str) -> str:
    """Obtiene el timestamp máximo en raw.orders (watermark_to del batch actual)."""
    sql = f"""
        SELECT MAX(SAFE_CAST(order_purchase_timestamp AS TIMESTAMP)) as max_ts
        FROM `{project}.raw.orders`
    """
    results = list(client.query(sql).result())
    return results[0].max_ts.strftime("%Y-%m-%d %H:%M:%S") if results[0].max_ts else None


def run_orders_incremental(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    watermark_from: str,
) -> dict:
    """
    Hace UPSERT de orders nuevas/actualizadas desde el watermark.
    Retorna métricas: new_count, updated_count, total.
    """
    log.info("── orders (MERGE incremental) ──")
    log.info("  Watermark desde: %s", watermark_from)

    # Contar nuevas órdenes en raw
    count_sql = f"""
        SELECT COUNT(*) as n
        FROM `{project}.raw.orders`
        WHERE SAFE_CAST(order_purchase_timestamp AS TIMESTAMP)
              > TIMESTAMP('{watermark_from}')
          AND order_id IS NOT NULL
    """
    delta_count = list(client.query(count_sql).result())[0].n
    log.info("  Órdenes delta en raw: %d", delta_count)

    if delta_count == 0:
        log.info("  Sin cambios nuevos.")
        return {"table_name": "orders", "raw_count": 0, "new_count": 0, "updated_count": 0}

    # Contar cuántas ya existen en curated (serán updates)
    overlap_sql = f"""
        SELECT COUNT(*) as n
        FROM `{project}.raw.orders` r
        INNER JOIN `{project}.curated.orders` c USING (order_id)
        WHERE SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', r.order_purchase_timestamp)
              > TIMESTAMP('{watermark_from}')
    """
    updated_count = list(client.query(overlap_sql).result())[0].n
    new_count = delta_count - updated_count

    # MERGE: upserta órdenes nuevas y actualiza existentes
    merge_sql = f"""
        MERGE `{project}.curated.orders` T
        USING (
            SELECT
                order_id,
                customer_id,
                UPPER(TRIM(order_status))                                            AS order_status,
                SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
                SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_approved_at)        AS order_approved_at,
                SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_carrier_date)  AS order_delivered_carrier_date,
                SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) AS order_delivered_customer_date,
                SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_estimated_delivery_date) AS order_estimated_delivery_date,
                '{batch_id}'       AS batch_id,
                CURRENT_TIMESTAMP() AS load_date,
                'olist_orders_dataset.csv' AS source_file
            FROM `{project}.raw.orders`
            WHERE
                SAFE_CAST(order_purchase_timestamp AS TIMESTAMP)
                    > TIMESTAMP('{watermark_from}')
                AND order_id IS NOT NULL
                AND customer_id IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) DESC
            ) = 1
        ) S
        ON T.order_id = S.order_id
        WHEN MATCHED THEN UPDATE SET
            order_status                  = S.order_status,
            order_approved_at             = S.order_approved_at,
            order_delivered_carrier_date  = S.order_delivered_carrier_date,
            order_delivered_customer_date = S.order_delivered_customer_date,
            order_estimated_delivery_date = S.order_estimated_delivery_date,
            batch_id                      = S.batch_id,
            load_date                     = S.load_date
        WHEN NOT MATCHED THEN INSERT VALUES (
            S.order_id,
            S.customer_id,
            S.order_status,
            S.order_purchase_timestamp,
            S.order_approved_at,
            S.order_delivered_carrier_date,
            S.order_delivered_customer_date,
            S.order_estimated_delivery_date,
            S.batch_id,
            S.load_date,
            S.source_file
        )
    """
    job = client.query(merge_sql)
    job.result()

    log.info("  ✓ MERGE completado: %d nuevas, %d actualizadas", new_count, updated_count)
    return {
        "table_name": "orders",
        "raw_count": delta_count,
        "new_count": new_count,
        "updated_count": updated_count,
    }


def run_reference_table_incremental(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    table: str,
    pk_column: str,
    curated_columns: list[str],
    select_sql: str,
) -> dict:
    """
    Para tablas de referencia: inserta solo registros no existentes aún en curated.
    """
    log.info("── %s (INSERT nuevos) ──", table)

    merge_sql = f"""
        MERGE `{project}.curated.{table}` T
        USING (
            {select_sql}
        ) S
        ON T.{pk_column} = S.{pk_column}
        WHEN NOT MATCHED THEN INSERT ({', '.join(curated_columns)})
        VALUES ({', '.join(f'S.{c}' for c in curated_columns)})
    """
    job = client.query(merge_sql)
    job.result()

    # Contar nuevos insertados
    count_sql = f"""
        SELECT COUNT(*) as n FROM `{project}.curated.{table}`
        WHERE batch_id = '{batch_id}'
    """
    new_count = list(client.query(count_sql).result())[0].n
    log.info("  ✓ %d registros nuevos en curated.%s", new_count, table)

    return {"table_name": table, "raw_count": new_count, "new_count": new_count, "updated_count": 0}


def write_batch_log(client, project, batch_id, status, started_at, finished_at,
                    watermark_from, watermark_to, tables_processed, total_rows, error_msg=None):
    table_ref = f"{project}.ops.batch_logs"
    row = {
        "batch_id": batch_id,
        "batch_type": "INCREMENTAL",
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "watermark_from": watermark_from,
        "watermark_to": watermark_to,
        "tables_processed": tables_processed,
        "total_rows_loaded": total_rows,
        "error_message": error_msg,
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        log.warning("Error escribiendo batch_logs: %s", errors)


def write_table_metrics(client, project, batch_id, metrics_list):
    table_ref = f"{project}.ops.table_metrics"
    rows = [
        {
            "batch_id": batch_id,
            "table_name": m["table_name"],
            "source_file": f"olist_{m['table_name']}_dataset.csv",
            "raw_row_count": m["raw_count"],
            "loaded_row_count": m.get("new_count", 0) + m.get("updated_count", 0),
            "rejected_row_count": 0,
            "new_row_count": m.get("new_count", 0),
            "updated_row_count": m.get("updated_count", 0),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        for m in metrics_list
    ]
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        log.warning("Error escribiendo table_metrics: %s", errors)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch incremental (delta): raw → curated con MERGE.")
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT"))
    parser.add_argument(
        "--cutoff",
        default=None,
        help="Watermark manual (YYYY-MM-DD). Si se omite, se usa el último batch exitoso.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.project:
        raise ValueError("Falta el proyecto GCP. Usa --project o define GCP_PROJECT en .env")

    client = bigquery.Client(project=args.project)
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    # Determinar watermark de inicio
    if args.cutoff:
        watermark_from = f"{args.cutoff} 00:00:00"
        log.info("Watermark manual: %s", watermark_from)
    else:
        watermark_from = get_last_watermark(client, args.project)
        if not watermark_from:
            watermark_from = DEFAULT_CUTOFF
            log.info("Sin batch previo. Usando cutoff por defecto: %s", watermark_from)
        else:
            log.info("Watermark del último batch: %s", watermark_from)

    watermark_to = get_max_timestamp(client, args.project)

    log.info("=" * 55)
    log.info("BATCH INCREMENTAL   batch_id=%s", batch_id)
    log.info("Watermark FROM: %s", watermark_from)
    log.info("Watermark TO  : %s", watermark_to)
    log.info("=" * 55)

    metrics_list = []
    error_msg = None
    status = "SUCCESS"

    try:
        # Tabla central: orders (con MERGE completo)
        metrics = run_orders_incremental(client, args.project, batch_id, watermark_from)
        metrics_list.append(metrics)

        # Tablas de ítems relacionados a las nuevas órdenes
        for table, pk, cols, sql in [
            (
                "order_items",
                "order_id",
                ["order_id", "order_item_id", "product_id", "seller_id",
                 "shipping_limit_date", "price", "freight_value", "total_value",
                 "batch_id", "load_date", "source_file"],
                f"""
                SELECT
                    oi.order_id,
                    CAST(oi.order_item_id AS INT64) AS order_item_id,
                    oi.product_id,
                    oi.seller_id,
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', oi.shipping_limit_date) AS shipping_limit_date,
                    ROUND(CAST(oi.price AS FLOAT64), 2) AS price,
                    ROUND(CAST(oi.freight_value AS FLOAT64), 2) AS freight_value,
                    ROUND(CAST(oi.price AS FLOAT64) + CAST(oi.freight_value AS FLOAT64), 2) AS total_value,
                    '{batch_id}' AS batch_id,
                    CURRENT_TIMESTAMP() AS load_date,
                    'olist_order_items_dataset.csv' AS source_file
                FROM `{args.project}.raw.order_items` oi
                INNER JOIN `{args.project}.raw.orders` o USING (order_id)
                WHERE SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP)
                      > TIMESTAMP('{watermark_from}')
                  AND oi.order_id IS NOT NULL AND oi.price IS NOT NULL
                """,
            ),
            (
                "order_payments",
                "order_id",
                ["order_id", "payment_sequential", "payment_type",
                 "payment_installments", "payment_value", "batch_id", "load_date", "source_file"],
                f"""
                SELECT
                    op.order_id,
                    CAST(op.payment_sequential AS INT64) AS payment_sequential,
                    UPPER(TRIM(op.payment_type)) AS payment_type,
                    CAST(op.payment_installments AS INT64) AS payment_installments,
                    ROUND(CAST(op.payment_value AS FLOAT64), 2) AS payment_value,
                    '{batch_id}' AS batch_id,
                    CURRENT_TIMESTAMP() AS load_date,
                    'olist_order_payments_dataset.csv' AS source_file
                FROM `{args.project}.raw.order_payments` op
                INNER JOIN `{args.project}.raw.orders` o USING (order_id)
                WHERE SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP)
                      > TIMESTAMP('{watermark_from}')
                  AND op.order_id IS NOT NULL AND op.payment_value IS NOT NULL
                """,
            ),
            (
                "order_reviews",
                "review_id",
                ["review_id", "order_id", "review_score", "review_comment_title",
                 "review_comment_message", "review_creation_date", "review_answer_timestamp",
                 "batch_id", "load_date", "source_file"],
                f"""
                SELECT
                    r.review_id,
                    r.order_id,
                    CAST(r.review_score AS INT64) AS review_score,
                    NULLIF(TRIM(r.review_comment_title), '') AS review_comment_title,
                    NULLIF(TRIM(r.review_comment_message), '') AS review_comment_message,
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', r.review_creation_date) AS review_creation_date,
                    SAFE_CAST(r.review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp,
                    '{batch_id}' AS batch_id,
                    CURRENT_TIMESTAMP() AS load_date,
                    'olist_order_reviews_dataset.csv' AS source_file
                FROM `{args.project}.raw.order_reviews` r
                INNER JOIN `{args.project}.raw.orders` o USING (order_id)
                WHERE SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP)
                      > TIMESTAMP('{watermark_from}')
                  AND r.review_id IS NOT NULL
                  AND CAST(r.review_score AS INT64) BETWEEN 1 AND 5
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY r.review_id
                    ORDER BY SAFE_CAST(r.review_answer_timestamp AS TIMESTAMP) DESC
                ) = 1
                """,
            ),
        ]:
            log.info("── %s (INSERT nuevos) ──", table)
            m = run_reference_table_incremental(client, args.project, batch_id, table, pk, cols, sql)
            metrics_list.append(m)

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        log.error("Pipeline incremental fallido: %s", exc)
        raise

    finally:
        finished_at = datetime.now(timezone.utc)
        total_rows = sum(
            m.get("new_count", 0) + m.get("updated_count", 0) for m in metrics_list
        )

        if metrics_list:
            write_table_metrics(client, args.project, batch_id, metrics_list)

        write_batch_log(
            client=client,
            project=args.project,
            batch_id=batch_id,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            watermark_from=watermark_from,
            watermark_to=watermark_to,
            tables_processed=len(metrics_list),
            total_rows=total_rows,
            error_msg=error_msg,
        )

    duration = (finished_at - started_at).total_seconds()
    log.info("=" * 55)
    log.info("INCREMENTAL FINALIZADO")
    log.info("Status  : %s", status)
    log.info("Tablas  : %d procesadas", len(metrics_list))
    log.info("Filas   : %d (nuevas + actualizadas)", total_rows)
    log.info("Duración: %.1f segundos", duration)
    log.info("=" * 55)


if __name__ == "__main__":
    main()
