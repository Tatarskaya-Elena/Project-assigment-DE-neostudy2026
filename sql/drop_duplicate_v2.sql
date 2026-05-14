WITH duplicates AS (
   SELECT
       ctid,
       client_rk,
       effective_from_date,
       ROW_NUMBER() OVER (
           PARTITION BY client_rk, effective_from_date
           ORDER BY effective_to_date DESC, ctid
       ) AS rn
   FROM dm.client
)
DELETE FROM dm.client
WHERE ctid IN (
   SELECT ctid FROM duplicates WHERE rn > 1
);

