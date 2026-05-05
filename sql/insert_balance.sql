WITH exchange_rates AS (
    SELECT 
        e.currency_rk,
        e.reduced_cource,
        ROW_NUMBER() OVER (PARTITION BY e.currency_rk ORDER BY e.data_actual_date DESC) AS rn
    FROM ds.md_exchange_rate_d e
    WHERE e.data_actual_date <= '2017-12-31' 
      AND e.data_actual_end_date >= '2017-12-31'
)

SELECT 
    f.account_rk,
    f.balance_out,
    COALESCE(f.balance_out * er.reduced_cource, f.balance_out) AS balance_out_rub,
    f.on_date 
FROM ds.ft_balance_f f 
LEFT JOIN ds.md_account_d a ON a.account_rk = f.account_rk
LEFT JOIN exchange_rates er ON er.currency_rk = a.currency_rk AND er.rn = 1
WHERE f.on_date = '2017-12-31';