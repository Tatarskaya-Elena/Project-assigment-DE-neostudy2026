DELETE FROM dm.client
WHERE (client_rk, effective_from_date, ctid) NOT IN (
   SELECT
       client_rk,
       effective_from_date,
       MIN(ctid)
   FROM dm.client
   GROUP BY client_rk, effective_from_date
);

