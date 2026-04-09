-- sql/curated/products.sql
-- Transformación: raw.products + raw.product_category_translation → curated.products
--
-- Transformaciones aplicadas:
--   - Join con tabla de traducción de categorías
--   - Valores numéricos casteados a INTEGER/FLOAT
--   - NULLs explícitos en campos opcionales
--   - Deduplicación por product_id

SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english, p.product_category_name) AS product_category_name_english,
    CAST(p.product_name_lenght AS INT64)                                AS product_name_length,
    CAST(p.product_description_lenght AS INT64)                         AS product_description_length,
    CAST(p.product_photos_qty AS INT64)                                 AS product_photos_qty,
    CAST(p.product_weight_g AS FLOAT64)                                 AS product_weight_g,
    CAST(p.product_length_cm AS FLOAT64)                                AS product_length_cm,
    CAST(p.product_height_cm AS FLOAT64)                                AS product_height_cm,
    CAST(p.product_width_cm AS FLOAT64)                                 AS product_width_cm,
    -- Trazabilidad
    @batch_id                                                           AS batch_id,
    CURRENT_TIMESTAMP()                                                 AS load_date,
    'olist_products_dataset.csv'                                        AS source_file
FROM `{project}.raw.products` p
LEFT JOIN `{project}.raw.product_category_translation` t
    ON p.product_category_name = t.product_category_name
WHERE
    p.product_id IS NOT NULL
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY p.product_id) = 1
