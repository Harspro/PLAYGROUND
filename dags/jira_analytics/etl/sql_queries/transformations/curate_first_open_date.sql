MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING (
    WITH candidate_issues AS (
        SELECT issue_key
        FROM `{{ curated.UNIQUE_ISSUES }}`
        WHERE resolution NOT IN (
            "Won't Do",
            "Duplicate",
            "Cancelled by Business"
        )
        AND issue_type IN (
            "epic",
            "story"
        )
    )

    SELECT
        issue_key,
        MIN(event_time) AS first_open_date
    FROM `{{ curated.STATUS_CHANGELOG }}`
    WHERE
        to_status IN (
            "In Progress",
            "In Development"
        )
        AND issue_key IN (SELECT * FROM candidate_issues)
    GROUP BY 1

) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN
    UPDATE SET
        ui.first_open_date = merge_source.first_open_date;
