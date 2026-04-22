CREATE OR REPLACE VIEW
  `{one_current_card_view_id}` OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
    description = "View for storing latest active card" ) AS (
  WITH
    CIF_CARD_COMBINED AS (
    SELECT
      {columns}
    FROM
      `{curr_transformed_data}`
    UNION ALL
    SELECT
      {columns}
    FROM
      `{source_table_id}`),
    CIF_CARD_LABELED AS (
    SELECT
      {columns},
      ROW_NUMBER() OVER(PARTITION BY {rank_partition} ORDER BY {rank_order_by}) AS ROW_RANK
    FROM
      CIF_CARD_COMBINED)
  SELECT
    DISTINCT {columns}
  FROM
    CIF_CARD_LABELED
  WHERE
    ROW_RANK = 1);