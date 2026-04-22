CREATE OR REPLACE TABLE `pcb-{env}-landing.domain_technical.DATA_LOADING_AGENDA` AS
WITH calendar AS (
SELECT
  calendar_date,
  CAST(EXTRACT(DAYOFWEEK FROM calendar_date) AS INT64) AS day_of_week,
FROM UNNEST(GENERATE_DATE_ARRAY('{start_date}', '{end_date}')) AS calendar_date
),
data_schedule AS(
SELECT
  table_name,
  dataset_id,
  source,
  frequency,
  buffer_in_minutes,
  business_cutoff_time,
  TIME(PARSE_TIME('%H:%M', TRIM(planned_schedule))) as planned_schedule
FROM `pcb-{env}-landing.domain_technical.TABLE_LOADING_SCHEDULES`
),
data_loading_agenda AS (
SELECT 
  data_schedule.table_name,
  data_schedule.dataset_id,
  data_schedule.source,
  data_schedule.frequency,
  data_schedule.planned_schedule,
  data_schedule.buffer_in_minutes,
  data_schedule.business_cutoff_time,
  calendar.calendar_date,
  DATETIME(calendar.calendar_date, data_schedule.planned_schedule) as planned_load_time,
CASE 
  WHEN data_schedule.frequency IN ('daily','hourly', 'half-hourly') THEN 'Y'
  -- Frequency 2-6 : Cron day of week (Tuesday-Saturday)
  WHEN data_schedule.frequency = '2-6' THEN
    CASE
      -- BigQuery day of week (1, 2) : (Sunday, Monday)
      WHEN calendar.day_of_week IN (1,2) THEN 'N'
      ELSE 'Y'
      END
  -- Frequency 1-5 : Cron day of week (Monday-Friday)
  WHEN data_schedule.frequency = '1-5' THEN
    CASE
      -- BigQuery day of week (1, 7) : (Sunday, Saturday)
      WHEN calendar.day_of_week IN (1,7) THEN 'N'
      ELSE 'Y'
      END
  WHEN data_schedule.frequency = 'sundays' THEN 
    CASE
      -- BigQuery day of week (Sunday = 1)
      WHEN calendar.day_of_week = 1 THEN 'Y'
      ELSE 'N'
      END
  -- BigQuery day of week (Monday = 2)
  WHEN data_schedule.frequency = 'mondays' THEN
    CASE
      WHEN calendar.day_of_week = 2 THEN 'Y'
      ELSE 'N'
      END
  -- Monthly frequency: Runs on the last day of each month
  WHEN data_schedule.frequency = 'monthly' THEN
    CASE 
      WHEN calendar_date = LAST_DAY(calendar_date) THEN 'Y'
      ELSE 'N'
      END    
  END as is_run_expected
FROM data_schedule 
CROSS JOIN
calendar
),
data_loading_agenda_with_holiday_indicator AS (
SELECT
  data_loading_agenda.table_name,
  data_loading_agenda.dataset_id,
  data_loading_agenda.source,
  data_loading_agenda.frequency,
  data_loading_agenda.planned_schedule,
  data_loading_agenda.buffer_in_minutes,
  data_loading_agenda.calendar_date,
  data_loading_agenda.planned_load_time,
  data_loading_agenda.is_run_expected,
  data_loading_agenda.business_cutoff_time,
  IF(hld.holiday_date IS NULL, 'N', 'Y') AS holiday_ind,
FROM
  data_loading_agenda
LEFT JOIN
`pcb-{env}-landing.domain_technical.VENDOR_HOLIDAY_DATES` hld ON
hld.holiday_date = data_loading_agenda.calendar_date
),
finalized_data_loading_agenda AS (
SELECT
  hol_ind.table_name,
  hol_ind.dataset_id,
  hol_ind.source,
  hol_ind.frequency,
  hol_ind.planned_schedule,
  hol_ind.planned_load_time,
  hol_ind.calendar_date,
  hol_ind.buffer_in_minutes,
  IF(hol_ind.holiday_ind = 'Y' and source = 'file', 'N', hol_ind.is_run_expected) as is_run_expected,
  hol_ind.business_cutoff_time
FROM data_loading_agenda_with_holiday_indicator hol_ind
)
select
  calendar_date,
  table_name,
  dataset_id,
  source,
  frequency,
  planned_schedule,
  planned_load_time,
  buffer_in_minutes,
  business_cutoff_time
FROM finalized_data_loading_agenda 
WHERE is_run_expected = 'Y'
ORDER BY calendar_date