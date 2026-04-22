SELECT
    *
FROM `{control_table_id}`
WHERE dagName = '{dag_id}'
ORDER BY windowEndTime DESC
LIMIT 1
