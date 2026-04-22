SELECT * EXCEPT(rn)
FROM (
  SELECT 
    id,
    is_active, 
    file_id, 
    target_location_id, 
    source_prefix, 
    target_prefix, 
    source_suffix, 
    target_suffix, 
    rename_pattern, 
    re_encrypt, 
    created_at, 
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY 
       id
      ORDER BY updated_at DESC
    ) AS rn
  FROM `pcb-{pnp_env}-compute.mft_ms.MS_DISTRIBUTION_LIST`
)
WHERE rn = 1