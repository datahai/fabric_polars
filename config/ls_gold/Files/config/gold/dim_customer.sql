-- type: dimension
-- business_key: customer_id
-- surrogate_key: customer_sk
-- scd: 2
-- history_columns: country, region
-- description: Customers. A change of country or region keeps history; name changes overwrite.
SELECT
    c.customer_id,
    c.customer_name,
    COALESCE(c.country, 'Not known')                     AS country,
    CASE
        WHEN c.country IN ('United Kingdom', 'Ireland')                     THEN 'UK & Ireland'
        WHEN c.country IN ('France', 'Germany', 'Netherlands', 'Spain')     THEN 'Europe'
        WHEN c.country = 'United States'                                    THEN 'North America'
        ELSE 'Not known'
    END                                                   AS region,
    CAST(c.created_at AS DATE)                            AS customer_since
FROM silver.customers AS c
