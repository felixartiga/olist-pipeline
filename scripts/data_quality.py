"""
data_quality.py  —  Parte E del assessment
─────────────────────────────────────────────────────────────
Ejecuta controles de calidad de datos sobre las tablas curated
y registra los resultados en ops.quality_checks.

Controles implementados (7 en total):
  1. [ERROR]   Campos obligatorios nulos en curated.orders
  2. [ERROR]   Estados de orden fuera del catálogo válido
  3. [ERROR]   Ítems huérfanos (order_id no existe en curated.orders)
  4. [WARNING] Duplicados lógicos en curated.orders
  5. [ERROR]   Fechas inválidas: entrega antes que la compra
  6. [WARNING] Pagos con valor cero o negativo
  7. [WARNING] Reviews con order_id sin orden correspondiente

Cada control retorna:
  - check_name, table_name, severity
  - total_checked, failed_count, pass_rate
  - status (PASS / FAIL / WARNING)
  - details

Uso:
    python scripts/data_quality.py --project my-gcp-project [--batch-id <id>]
"""

import argparse
import logging
import os
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Definición de controles de calidad ───────────────────────────────────────

def check_null_mandatory_fields(client: bigquery.Client, project: str) -> dict:
    """Control 1: Campos obligatorios nulos en orders (order_id, customer_id, order_status)."""
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(order_id IS NULL OR customer_id IS NULL OR order_status IS NULL) AS failed
        FROM `{project}.curated.orders`
    """
    row = list(client.query(sql).result())[0]
    total, failed = row.total, row.failed
    pass_rate = 1.0 - (failed / total) if total > 0 else 1.0

    return {
        "check_name": "null_mandatory_fields_orders",
        "table_name": "curated.orders",
        "severity": "ERROR",
        "total_checked": total,
        "failed_count": failed,
        "pass_rate": round(pass_rate, 4),
        "status": "PASS" if failed == 0 else "FAIL",
        "details": f"{failed} registros con order_id, customer_id o order_status nulos.",
    }


def check_invalid_order_status(client: bigquery.Client, project: str) -> dict:
    """Control 2: Estados de orden fuera del catálogo válido."""
    valid_statuses = (
        "'DELIVERED', 'SHIPPED', 'CANCELED', 'UNAVAILABLE', "
        "'INVOICED', 'PROCESSING', 'CREATED', 'APPROVED'"
    )
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(order_status NOT IN ({valid_statuses})) AS failed
        FROM `{project}.curated.orders`
        WHERE order_status IS NOT NULL
    """
    row = list(client.query(sql).result())[0]
    total, failed = row.total, row.failed
    pass_rate = 1.0 - (failed / total) if total > 0 else 1.0

    return {
        "check_name": "invalid_order_status",
        "table_name": "curated.orders",
        "severity": "ERROR",
        "total_checked": total,
        "failed_count": failed,
        "pass_rate": round(pass_rate, 4),
        "status": "PASS" if failed == 0 else "FAIL",
        "details": f"{failed} órdenes con status fuera del catálogo: {{{valid_statuses}}}",
    }


def check_orphan_order_items(client: bigquery.Client, project: str) -> dict:
    """Control 3: Ítems huérfanos (order_id no existe en curated.orders)."""
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(o.order_id IS NULL) AS failed
        FROM `{project}.curated.order_items` oi
        LEFT JOIN `{project}.curated.orders` o USING (order_id)
    """
    row = list(client.query(sql).result())[0]
    total, failed = row.total, row.failed
    pass_rate = 1.0 - (failed / total) if total > 0 else 1.0

    return {
        "check_name": "orphan_order_items",
        "table_name": "curated.order_items",
        "severity": "ERROR",
        "total_checked": total,
        "failed_count": failed,
        "pass_rate": round(pass_rate, 4),
        "status": "PASS" if failed == 0 else "FAIL",
        "details": f"{failed} ítems con order_id sin orden correspondiente en curated.orders.",
    }


def check_duplicate_orders(client: bigquery.Client, project: str) -> dict:
    """Control 4: Duplicados lógicos en orders (mismo order_id más de una vez)."""
    sql = f"""
        SELECT
            COUNT(*) AS total,
            SUM(IF(cnt > 1, cnt, 0)) AS failed
        FROM (
            SELECT order_id, COUNT(*) AS cnt
            FROM `{project}.curated.orders`
            GROUP BY order_id
        )
    """
    row = list(client.query(sql).result())[0]
    total, failed = row.total, (row.failed or 0)
    pass_rate = 1.0 - (failed / total) if total > 0 else 1.0

    return {
        "check_name": "duplicate_order_ids",
        "table_name": "curated.orders",
        "severity": "WARNING",
        "total_checked": total,
        "failed_count": failed,
        "pass_rate": round(pass_rate, 4),
        "status": "PASS" if failed == 0 else "WARNING",
        "details": f"{failed} registros que comparten un order_id duplicado.",
    }


def check_invalid_dates(client: bigquery.Client, project: str) -> dict:
    """Control 5: Fechas inválidas — entrega antes que la compra o aprobación antes que compra."""
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(
                (order_delivered_customer_date IS NOT NULL
                 AND order_delivered_customer_date < order_purchase_timestamp)
                OR
                (order_approved_at IS NOT NULL
                 AND order_approved_at < order_purchase_timestamp)
            ) AS failed
        FROM `{project}.curated.orders`
        WHERE order_purchase_timestamp IS NOT NULL
    """
    row = list(client.query(sql).result())[0]
    total, failed = row.total, row.failed
    pass_rate = 1.0 - (failed / total) if total > 0 else 1.0

    return {
        "check_name": "invalid_date_sequence",
        "table_name": "curated.orders",
        "severity": "ERROR",
        "total_checked": total,
        "failed_count": failed,
        "pass_rate": round(pass_rate, 4),
        "status": "PASS" if failed == 0 else "FAIL",
        "details": (
            f"{failed} órdenes con fechas inconsistentes "
            "(entrega < compra o aprobación < compra)."
        ),
    }


def check_zero_payments(client: bigquery.Client, project: str) -> dict:
    """Control 6: Pagos con valor cero o negativo."""
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(payment_value <= 0) AS failed
        FROM `{project}.curated.order_payments`
        WHERE payment_value IS NOT NULL
    """
    row = list(client.query(sql).result())[0]
    total, failed = row.total, row.failed
    pass_rate = 1.0 - (failed / total) if total > 0 else 1.0

    return {
        "check_name": "zero_or_negative_payment",
        "table_name": "curated.order_payments",
        "severity": "WARNING",
        "total_checked": total,
        "failed_count": failed,
        "pass_rate": round(pass_rate, 4),
        "status": "PASS" if failed == 0 else "WARNING",
        "details": f"{failed} pagos con valor <= 0.",
    }


def check_orphan_reviews(client: bigquery.Client, project: str) -> dict:
    """Control 7: Reviews con order_id sin orden en curated.orders."""
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(o.order_id IS NULL) AS failed
        FROM `{project}.curated.order_reviews` r
        LEFT JOIN `{project}.curated.orders` o USING (order_id)
    """
    row = list(client.query(sql).result())[0]
    total, failed = row.total, row.failed
    pass_rate = 1.0 - (failed / total) if total > 0 else 1.0

    return {
        "check_name": "orphan_reviews",
        "table_name": "curated.order_reviews",
        "severity": "WARNING",
        "total_checked": total,
        "failed_count": failed,
        "pass_rate": round(pass_rate, 4),
        "status": "PASS" if failed == 0 else "WARNING",
        "details": f"{failed} reviews cuyo order_id no existe en curated.orders.",
    }


CHECKS = [
    check_null_mandatory_fields,
    check_invalid_order_status,
    check_orphan_order_items,
    check_duplicate_orders,
    check_invalid_dates,
    check_zero_payments,
    check_orphan_reviews,
]


# ── Escritura en ops ──────────────────────────────────────────────────────────

def write_quality_results(
    client: bigquery.Client,
    project: str,
    batch_id: str,
    results: list[dict],
) -> None:
    table_ref = f"{project}.ops.quality_checks"
    checked_at = datetime.now(timezone.utc).isoformat()
    rows = [
        {**r, "batch_id": batch_id, "checked_at": checked_at}
        for r in results
    ]
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        log.warning("Error escribiendo quality_checks: %s", errors)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controles de calidad de datos — Olist pipeline.")
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT"))
    parser.add_argument(
        "--batch-id",
        default=None,
        help="batch_id a asociar. Si se omite, genera uno nuevo.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.project:
        raise ValueError("Falta el proyecto GCP. Usa --project o define GCP_PROJECT en .env")

    client = bigquery.Client(project=args.project)
    batch_id = args.batch_id or str(uuid.uuid4())

    log.info("=" * 55)
    log.info("CONTROLES DE CALIDAD   batch_id=%s", batch_id)
    log.info("=" * 55)

    results = []
    fail_count = 0

    for check_fn in CHECKS:
        try:
            result = check_fn(client, args.project)
            results.append(result)
            icon = "✓" if result["status"] == "PASS" else ("⚠" if result["status"] == "WARNING" else "✗")
            log.info(
                "  %s [%s] %s — %d/%d fallos (pass_rate=%.2f%%)",
                icon,
                result["severity"],
                result["check_name"],
                result["failed_count"],
                result["total_checked"],
                result["pass_rate"] * 100,
            )
            if result["status"] == "FAIL":
                fail_count += 1
        except Exception as exc:
            log.error("  ✗ Error en control '%s': %s", check_fn.__name__, exc)

    write_quality_results(client, args.project, batch_id, results)

    log.info("─" * 55)
    log.info("Controles ejecutados : %d", len(results))
    log.info("PASS                 : %d", sum(1 for r in results if r["status"] == "PASS"))
    log.info("WARNING              : %d", sum(1 for r in results if r["status"] == "WARNING"))
    log.info("FAIL                 : %d", fail_count)
    log.info("Resultados guardados en ops.quality_checks")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
