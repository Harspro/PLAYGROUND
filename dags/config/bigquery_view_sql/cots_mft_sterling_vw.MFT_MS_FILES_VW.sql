SELECT * EXCEPT(rn)
FROM (
  SELECT
    id, 
    pattern, 
    description, 
    is_active, 
    source_node_id, 
    location_id, 
    archive_name_pattern, 
    created_at, 
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY 
       id
      ORDER BY updated_at DESC
    ) AS rn
  FROM `pcb-{pnp_env}-compute.mft_ms.MS_FILES`
)
WHERE rn = 1