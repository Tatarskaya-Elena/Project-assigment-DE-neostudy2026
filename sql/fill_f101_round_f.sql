SELECT 
    DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 month' AS FROM_DATE,
    DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 day' AS TO_DATE,
    l.chapter AS CHAPTER,
    SUBSTRING(a.account_number, 1, 5) AS LEDGER_ACCOUNT,
    a.char_type AS CHARACTERISTIC,
    SUM(CASE 
            WHEN a.currency_code IN ('810', '643') AND b.on_date = DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 month' - INTERVAL '1 day' THEN b.balance_out_rub 
            ELSE 0 
        END) AS BALANCE_IN_RUB,
    NULL AS R_BALANCE_IN_RUB,    
    SUM(CASE 
            WHEN a.currency_code NOT IN ('810', '643') AND b.on_date = DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 month' - INTERVAL '1 day' THEN b.balance_out_rub 
            ELSE 0 
        END) AS BALANCE_IN_VAL,
    NULL AS R_BALANCE_IN_VAL,    
       SUM(CASE 
            WHEN b.on_date = DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 month' - INTERVAL '1 day' THEN b.balance_out_rub 
            ELSE 0 
        END) AS BALANCE_IN_TOTAL,
    NULL AS R_BALANCE_IN_TOTAL,
    SUM(CASE 
            WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub 
            ELSE 0 
        END) AS TURN_DEB_RUB,
    NULL AS R_TURN_DEB_RUB,    
    SUM(CASE 
            WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub 
            ELSE 0 
        END) AS TURN_DEB_VAL,
    NULL AS R_TURN_DEB_VAL,   
    SUM(t.debet_amount_rub) AS TURN_DEB_TOTAL,
    NULL AS R_TURN_DEB_TOTAL, 
    SUM(CASE 
            WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub 
            ELSE 0 
        END) AS TURN_CRE_RUB,
    NULL AS R_TURN_CRE_RUB,    
      SUM(CASE 
            WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub 
            ELSE 0 
        END) AS TURN_CRE_VAL,
    NULL AS R_TURN_CRE_VAL,     
    SUM(t.credit_amount_rub) AS TURN_CRE_TOTAL,
    NULL AS R_TURN_CRE_TOTAL,
    SUM(CASE 
            WHEN a.currency_code IN ('810', '643') AND b.on_date = DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 day' THEN b.balance_out_rub 
            ELSE 0 
        END) AS BALANCE_OUT_RUB,
    NULL AS R_BALANCE_OUT_RUB,    
    SUM(CASE 
            WHEN a.currency_code NOT IN ('810', '643') AND b.on_date = DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 day' THEN b.balance_out_rub 
            ELSE 0 
        END) AS BALANCE_OUT_VAL,
    NULL AS R_BALANCE_OUT_VAL,     
    SUM(CASE 
            WHEN b.on_date = DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 day' THEN b.balance_out_rub 
            ELSE 0 
        END) AS BALANCE_OUT_TOTAL,
    NULL AS R_BALANCE_OUT_TOTAL
    FROM 
    ds.md_account_d a
LEFT JOIN 
    dm.dm_account_balance_f b ON a.account_rk = b.account_rk AND (b.on_date = DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 month' - INTERVAL '1 day' OR b.on_date = DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 day')
LEFT JOIN 
    dm.dm_account_turnover_f t ON a.account_rk = t.account_rk AND t.on_date BETWEEN DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 month' AND DATE_TRUNC('month', '{date}'::DATE) - INTERVAL '1 day'
JOIN 
    ds.md_ledger_account_s l ON SUBSTRING(a.account_number, 1, 5) = l.ledger_account::TEXT
GROUP BY 
    SUBSTRING(a.account_number, 1, 5),
    l.chapter,
    a.char_type;
    
