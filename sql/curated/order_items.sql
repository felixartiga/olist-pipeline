-- sql/curated/order_items.sql
-- Transformación: raw.order_items → curated.order_items
--
-- Transformaciones aplicadas:
--   - Parseo de shipping_limit_date
--   - Precio y flete redondeados a 2 decimales
--   - Filtro de PKs nulas y precios negativos o nulos
--   - Deduplicación por (order_id, order_item_id)

SELECT
    order_id,
    CAST(order_item_id AS INT64)                                             AS order_item_id,
    product_id,
    seller_id,
    SAFE_CAST(shipping_limit_date AS TIMESTAMP)                              AS shipping_limit_date,
    ROUND(CAST(price AS FLOAT64), 2)                                         AS price,
    ROUND(CAST(freight_value AS FLOAT64), 2)                                 AS freight_value,
    ROUND(CAST(price AS FLOAT64) + CAST(freight_value AS FLOAT64), 2)        AS total_value,
    -- Trazabilidad
    @batch_id                                                                AS batch_id,
    CURRENT_TIMESTAMP()                                                      AS load_date,
    'olist_order_items_dataset.csv'                                          AS source_file
FROM `{project}.raw.order_items`
WHERE
    order_id    IS NOT NULL
    AND product_id  IS NOT NULL
    AND seller_id   IS NOT NULL
    AND price       IS NOT NULL
    AND CAST(price AS FLOAT64) >= 0
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY order_id, order_item_id
        ORDER BY CAST(price AS FLOAT64) DESC
    ) = 1
