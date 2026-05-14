WITH duplicates AS (
   SELECT
       client_rk,
       effective_from_date
   FROM dm.client
   GROUP BY client_rk, effective_from_date
   HAVING COUNT(*) > 1
)
SELECT
   c.client_rk,
   c.effective_from_date,
   c.effective_to_date,
   c.client_id,
   c.card_type_code,
   c.counterparty_type_cd,
   c.black_list_flag,
   c.client_open_dttm,
   c.bankruptcy_rk,
   c.account_rk,
   c.address_rk,
   c.department_rk,
   ROW_NUMBER() OVER (PARTITION BY c.client_rk, c.effective_from_date ORDER BY c.effective_to_date DESC) AS dup_num
FROM dm.client c
INNER JOIN duplicates d ON c.client_rk = d.client_rk AND c.effective_from_date = d.effective_from_date
ORDER BY c.client_rk, c.effective_from_date, c.effective_to_date DESC;

