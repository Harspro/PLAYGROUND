MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING (
    SELECT
        issue_key,
        CASE
            WHEN due_date IS NOT NULL THEN due_date
            WHEN total_size != 0
                THEN
                    TIMESTAMP_ADD(
                        target_start_date,
                        INTERVAL CAST(ROUND(total_size * 7) AS int64) DAY
                    )
        END AS target_completion_date
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        issue_type = "initiative"
        AND target_start_date IS NOT NULL
) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN UPDATE SET
    ui.target_completion_date = merge_source.target_completion_date;
