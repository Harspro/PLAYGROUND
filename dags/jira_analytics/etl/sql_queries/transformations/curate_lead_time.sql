MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING (WITH lead_time_in_days AS (
    SELECT
        issue_key,
        TIMESTAMP_DIFF(
            COALESCE(target_start_date, CURRENT_TIMESTAMP()),
            created_date,
            DAY
        ) AS lead_time
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE issue_type IN ("initiative", "epic")
)

SELECT
    issue_key,
    ROUND(lead_time / 7, 1) AS lead_time
FROM lead_time_in_days) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN UPDATE SET
    ui.lead_time = merge_source.lead_time;
