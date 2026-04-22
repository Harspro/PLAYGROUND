MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING (
    WITH candidate_issues AS (
        SELECT issue_key
        FROM `{{ curated.UNIQUE_ISSUES }}`
        WHERE status IN ("Done", "Closed")
    )

    SELECT
        issue_key,
        MAX(event_time) AS final_close_date
    FROM `{{ curated.STATUS_CHANGELOG }}`
    WHERE
        to_status IN ("Done", "Closed")
        AND issue_key IN (SELECT * FROM candidate_issues)
    GROUP BY 1
) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN UPDATE SET
    ui.final_close_date = merge_source.final_close_date;
