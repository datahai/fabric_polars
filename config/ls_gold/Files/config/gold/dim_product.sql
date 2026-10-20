-- type: dimension
-- business_key: product_id
-- surrogate_key: product_sk
-- scd: 1
-- description: Products. Changes overwrite (no history).
SELECT
    p.product_id,
    p.product_name,
    p.unit_price,
    CASE
        WHEN p.unit_price IS NULL THEN 'Not priced'
        WHEN p.unit_price < 50    THEN 'Under 50'
        WHEN p.unit_price < 200   THEN '50 to 200'
        ELSE '200 and over'
    END                                                   AS price_band
FROM silver.products AS p
