SELECT 
    '{on_date}'::DATE AS on_date,
    account_rk,
    SUM(posting_c.credit_amount) AS credit_amount,
    SUM(posting_c.credit_amount * COALESCE(exchange.reduced_cource, 1)) AS credit_amount_rub,
    SUM(posting_d.debet_amount) AS debet_amount,
    SUM(posting_d.debet_amount * COALESCE(exchange.reduced_cource, 1)) AS debet_amount_rub
FROM 
    ds.md_account_d account_d
LEFT JOIN 
    ds.ft_posting_f posting_d ON posting_d.debet_account_rk=account_d.account_rk AND posting_d.oper_date = '{on_date}'::DATE
LEFT JOIN 
    ds.ft_posting_f posting_c ON posting_c.credit_account_rk = account_d.account_rk AND posting_c.oper_date = '{on_date}'::DATE
LEFT JOIN 
    (SELECT 
         currency_rk, 
         reduced_cource 
     FROM 
         ds.md_exchange_rate_d 
     WHERE 
         data_actual_date < '{on_date}'::DATE AND 
         data_actual_end_date > '{on_date}'::DATE
    ) exchange ON exchange.currency_rk = account_d.currency_rk
WHERE 
   '{on_date}'::DATE BETWEEN account_d.data_actual_date AND account_d.data_actual_end_date
GROUP BY 
    account_rk
HAVING 
    SUM(posting_c.credit_amount) > 0 OR
    SUM(posting_d.debet_amount) > 0;
