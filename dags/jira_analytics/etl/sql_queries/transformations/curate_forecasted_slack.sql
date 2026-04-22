MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING
    (
        SELECT
            issue_key,
            ROUND(
                TIMESTAMP_DIFF(
                    target_completion_date,
                    forecasted_completion_date,
                    DAY
                ) / 7, 1
            ) AS forecasted_slack
        FROM `{{ curated.UNIQUE_ISSUES }}`
        WHERE
            forecasted_completion_date IS NOT NULL
            AND target_completion_date IS NOT NULL
    ) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN UPDATE SET
    ui.forecasted_slack = merge_source.forecasted_slack;
