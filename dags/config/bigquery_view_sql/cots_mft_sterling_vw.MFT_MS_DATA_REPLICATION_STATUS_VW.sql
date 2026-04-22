SELECT * EXCEPT(rn)
FROM (
  SELECT
    id,
    source_db,
    source_table,
    source_type,
    target_db,
    target_table,
    target_type,
    status,
    control_field,
    last_id,
    created_at,
    updated_at,
    last_updated_at,
    pod_name,
    pod_uid,
    ROW_NUMBER() OVER (
      PARTITION BY 
       id
      ORDER BY updated_at DESC
    ) AS rn
  FROM `pcb-{pnp_env}-compute.mft_ms.MS_DATA_REPLICATION_STATUS`
)
WHERE rn = 1