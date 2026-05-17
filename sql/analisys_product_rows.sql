SELECT
    product_rk,
    effective_from_date,
    COUNT(*)as cnt_rows
FROM rd.product
GROUP BY effective_from_date, product_rk
HAVING COUNT(*) >1;
