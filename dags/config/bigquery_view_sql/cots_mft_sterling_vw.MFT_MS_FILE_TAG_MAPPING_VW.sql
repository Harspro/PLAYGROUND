SELECT * EXCEPT(rn)
FROM (
  SELECT
    id,
    tag_id,
    file_id,
    created_at,
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY 
       id
      ORDER BY updated_at DESC
    ) AS rn
  FROM `pcb-{pnp_env}-compute.mft_ms.MS_FILE_TAG_MAPPING`
)
WHERE rn = 1