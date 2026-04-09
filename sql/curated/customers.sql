-- sql/curated/customers.sql
-- Transformación: raw.customers → curated.customers
--
-- Transformaciones aplicadas:
--   - Ciudad y estado estandarizados (INITCAP / UPPER)
--   - ZIP code como STRING con ceros a la izquierda preservados
--   - Filtro de PKs nulas
--   - Deduplicación por customer_id

SELECT
    customer_id,
    customer_unique_id,
    LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0') AS customer_zip_code_prefix,
    INITCAP(TRIM(customer_city))                           AS customer_city,
    UPPER(TRIM(customer_state))                            AS customer_state,
    -- Trazabilidad
    @batch_id                                              AS batch_id,
    CURRENT_TIMESTAMP()                                    AS load_date,
    'olist_customers_dataset.csv'                          AS source_file
FROM `{project}.raw.customers`
WHERE
    customer_id        IS NOT NULL
    AND customer_unique_id IS NOT NULL
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1
