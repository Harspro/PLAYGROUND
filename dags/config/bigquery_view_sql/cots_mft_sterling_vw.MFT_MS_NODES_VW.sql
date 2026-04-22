SELECT * EXCEPT(rn)
FROM (
  SELECT
    id, 
    name, 
    description, 
    type, 
    user_name, 
    secret_path, 
    gcs_project_id, 
    gcs_bucket, 
    is_active, 
    host, 
    port, 
    auth_method, 
    is_archival, 
    created_at, 
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY 
       id
      ORDER BY updated_at DESC
    ) AS rn
  FROM `pcb-{pnp_env}-compute.mft_ms.MS_NODES`
)
WHERE rn = 1