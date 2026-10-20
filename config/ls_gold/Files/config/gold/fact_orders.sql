-- type: fact
-- business_key: order_id
-- load: merge
-- description: One row per order. Customer is looked up as at the order date (SCD2).
SELECT
    o.order_id,
    COALESCE(c.customer_sk, -1)                           AS customer_sk,
    COALESCE(d.date_sk, -1)                               AS order_date_sk,
    o.amount
FROM silver.orders AS o
LEFT JOIN gold.dim_customer AS c
       ON c.customer_id = o.customer_id
      AND o.order_date >= c.valid_from
      AND o.order_date <  c.valid_to
LEFT JOIN gold.dim_date AS d
       ON d.date = o.order_date
WHERE o._silver_updated_utc > $watermark
