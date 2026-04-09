-- sql/curated/order_reviews.sql
-- Transformación: raw.order_reviews → curated.order_reviews
--
-- Transformaciones aplicadas:
--   - Parseo de fechas
--   - review_score validado entre 1 y 5
--   - Textos opcionales: NULL si vacíos
--   - Deduplicación: si un review aparece duplicado, se conserva el más reciente

SELECT
    review_id,
    order_id,
    CAST(review_score AS INT64)                                              AS review_score,
    NULLIF(TRIM(review_comment_title), '')                                   AS review_comment_title,
    NULLIF(TRIM(review_comment_message), '')                                 AS review_comment_message,
    SAFE_CAST(review_creation_date AS TIMESTAMP)                             AS review_creation_date,
    SAFE_CAST(review_answer_timestamp AS TIMESTAMP)                          AS review_answer_timestamp,
    -- Trazabilidad
    @batch_id                                                                AS batch_id,
    CURRENT_TIMESTAMP()                                                      AS load_date,
    'olist_order_reviews_dataset.csv'                                        AS source_file
FROM `{project}.raw.order_reviews`
WHERE
    review_id    IS NOT NULL
    AND order_id     IS NOT NULL
    AND review_score IS NOT NULL
    AND CAST(review_score AS INT64) BETWEEN 1 AND 5
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY review_id
        ORDER BY SAFE_CAST(review_answer_timestamp AS TIMESTAMP) DESC
    ) = 1
