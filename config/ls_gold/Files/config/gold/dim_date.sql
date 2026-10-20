-- type: dimension
-- business_key: date_sk
-- surrogate_key: none
-- description: Calendar 2020-2030. Key is yyyymmdd; -1 = unknown date.
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER)               AS date_sk,
    CAST(d AS DATE)                                       AS date,
    year(d)                                               AS year,
    quarter(d)                                            AS quarter,
    month(d)                                              AS month,
    strftime(d, '%B')                                     AS month_name,
    strftime(d, '%b %Y')                                  AS month_label,
    day(d)                                                AS day_of_month,
    isodow(d)                                             AS day_of_week,
    strftime(d, '%A')                                     AS day_name,
    isodow(d) >= 6                                        AS is_weekend,
    CASE WHEN month(d) >= 4 THEN year(d) ELSE year(d) - 1 END AS fiscal_year   -- UK fiscal year starts April
FROM range(DATE '2020-01-01', DATE '2031-01-01', INTERVAL 1 DAY) AS t(d)
