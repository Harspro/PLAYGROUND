SELECT * EXCEPT(rn)
FROM (
  SELECT
    id, 
    name, 
    path, 
    is_active, 
    node_id, 
    is_encrypted, 
    chmod, 
    created_at, 
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY 
       id
      ORDER BY updated_at DESC
    ) AS rn
  FROM `pcb-{pnp_env}-compute.mft_ms.MS_LOCATIONS`
)
WHERE rn = 1