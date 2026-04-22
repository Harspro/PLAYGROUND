WITH
  data_freshness AS (
  SELECT
    project_id,
    dataset_id,
    table_name,
    last_loaded_time,
    loaded_by
  FROM
    `pcb-{env}-landing.domain_data_logs.DATA_FRESHNESS` ),
  agenda_tbl AS (
  SELECT
    table_name,
    dataset_id,
    planned_load_time,
    buffer_in_minutes,
    business_cutoff_time
  FROM
    `pcb-{env}-landing.domain_technical.DATA_LOADING_AGENDA`
  WHERE
    calendar_date = CURRENT_DATE('America/Toronto') ),
  load_time_difference AS (
  SELECT
    df.project_id,
    df.dataset_id,
    df.table_name,
    df.loaded_by,
    df.last_loaded_time,
    agenda.planned_load_time,
    agenda.buffer_in_minutes,
    agenda.business_cutoff_time,
    ABS(DATETIME_DIFF(df.last_loaded_time, COALESCE(agenda.planned_load_time, CURRENT_DATETIME('America/Toronto')), MINUTE)) AS time_diff_minutes
  FROM
    data_freshness df
  LEFT JOIN
    agenda_tbl agenda
  ON
    df.dataset_id = agenda.dataset_id
    AND df.table_name = agenda.table_name ),
  minimum_load_time_difference AS (
  SELECT
    project_id,
    dataset_id,
    table_name,
    loaded_by,
    last_loaded_time,
    time_diff_minutes,
    buffer_in_minutes,
    business_cutoff_time,
    ROW_NUMBER() OVER (PARTITION BY project_id, dataset_id, table_name, loaded_by, last_loaded_time ORDER BY time_diff_minutes) AS rn
  FROM
    load_time_difference ),
  data_load_timeline AS (
  SELECT
    project_id,
    dataset_id,
    table_name,
    loaded_by,
    last_loaded_time,
    business_cutoff_time,
    IF(min_diff.time_diff_minutes <= buffer_in_minutes, 'On-Time', 'Delayed') AS data_load_status,
  FROM
    minimum_load_time_difference min_diff
  WHERE
    rn = 1 )
SELECT
  project_id,
  dataset_id,
  table_name,
  loaded_by,
  last_loaded_time,
  data_load_status,
  business_cutoff_time
FROM
  data_load_timeline