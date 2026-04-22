SELECT * EXCEPT(rn)
FROM (
  SELECT
    id, 
    file_id, 
    target_location, 
    type, 
    key_id, 
    version, 
    start_date, 
    end_date, 
    is_active, 
    created_at, 
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY 
       id
      ORDER BY updated_at DESC
    ) AS rn
  FROM `pcb-{pnp_env}-compute.mft_ms.MS_FILE_KEY_MAPPING`
)
WHERE rn = 1