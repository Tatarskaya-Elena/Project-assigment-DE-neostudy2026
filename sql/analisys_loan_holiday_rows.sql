SELECT
    deal_rk,
    effective_from_date,
    COUNT(*)as cnt_rows
FROM rd.loan_holiday
GROUP BY effective_from_date, deal_rk
HAVING COUNT(*) >1;
