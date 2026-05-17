SELECT   
    effective_from_date,   
    effective_to_date,
    COUNT(*)as cnt_rows
FROM rd.product
GROUP BY effective_from_date, effective_to_date
ORDER BY effective_from_date;
