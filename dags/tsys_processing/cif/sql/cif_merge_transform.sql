CREATE OR REPLACE VIEW
  `{transform_view_id}` OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
    description = "View for union transformation" ) AS (
  WITH
    CIF_TABLE AS (
    SELECT
      {source_columns}
    FROM
      `{source_table_id}`
    UNION ALL
    SELECT
      {source_columns}
    FROM
      `{target_table_id}`),
    CIF_CURR AS (
    SELECT
      {target_columns},
      ROW_NUMBER() OVER(PARTITION BY {rank_partition} ORDER BY {rank_order_by}) AS ROW_RANK
    FROM
      CIF_TABLE )
  SELECT
    DISTINCT {target_columns}
  FROM
    CIF_CURR
  WHERE
    ROW_RANK = 1 );