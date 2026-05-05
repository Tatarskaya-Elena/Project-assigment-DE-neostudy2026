SELECT 
    d.account_rk,
    COALESCE(b.balance_out, 0) + 
        CASE 
            WHEN d.char_type = 'А' THEN COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            WHEN d.char_type = 'П' THEN -COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
        END AS balance_out,
    COALESCE(b.balance_out_rub, 0) + 
        CASE 
            WHEN d.char_type = 'А' THEN COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
            WHEN d.char_type = 'П' THEN -COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub,
        '{on_date}'::DATE AS on_date
FROM ds.md_account_d d
LEFT JOIN dm.dm_account_balance_f b ON d.account_rk = b.account_rk AND b.on_date = '{on_date}'::DATE - INTERVAL '1 day'
LEFT JOIN (
    SELECT account_rk, 
           debet_amount ,
           credit_amount,
           debet_amount_rub, 
           credit_amount_rub
    FROM dm.dm_account_turnover_f
    WHERE on_date = '{on_date}'::DATE
) t ON d.account_rk = t.account_rk
WHERE d.data_actual_date <= '{on_date}'::DATE AND d.data_actual_end_date >= '{on_date}'::DATE;