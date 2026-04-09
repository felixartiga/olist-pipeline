-- sql/curated/orders.sql
-- Transformación: raw.orders → curated.orders
--
-- Transformaciones aplicadas:
--   - Parseo de timestamps (strings → TIMESTAMP)
--   - Estandarización de order_status (UPPER + TRIM)
--   - Filtro de registros con PK nula
--   - Deduplicación: si un order_id aparece duplicado, se conserva el más reciente
--   - Campos de trazabilidad: batch_id, load_date, source_file

SELECT
    order_id,
    customer_id,
    UPPER(TRIM(order_status))                                           AS order_status,
    SAFE_CAST(order_purchase_timestamp AS TIMESTAMP)                    AS order_purchase_timestamp,
    SAFE_CAST(order_approved_at AS TIMESTAMP)                           AS order_approved_at,
    SAFE_CAST(order_delivered_carrier_date AS TIMESTAMP)                AS order_delivered_carrier_date,
    SAFE_CAST(order_delivered_customer_date AS TIMESTAMP)               AS order_delivered_customer_date,
    SAFE_CAST(order_estimated_delivery_date AS TIMESTAMP)               AS order_estimated_delivery_date,
    -- Campos de trazabilidad
    @batch_id                                                           AS batch_id,
    CURRENT_TIMESTAMP()                                                 AS load_date,
    'olist_orders_dataset.csv'                                          AS source_file
FROM `{project}.raw.orders`
WHERE
    order_id    IS NOT NULL
    AND customer_id IS NOT NULL
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) DESC
    ) = 1
