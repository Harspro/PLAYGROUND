SELECT * EXCEPT(rn)
FROM (
  SELECT
    id, 
    node_id, 
    node_name, 
    location_id, 
    location_name, 
    file_name, 
    file_id, 
    status, 
    file_hash, 
    session_id, 
    retry_count, 
    replay, 
    replay_mode, 
    replay_target_id, 
    pod_name, 
    pod_uid,
    created_at, 
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY 
       id
      ORDER BY updated_at DESC
    ) AS rn
  FROM `pcb-{pnp_env}-compute.mft_ms.MS_PROCESS_QUEUE`
)
WHERE rn = 1