MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING (
    -- IF start_date IS NOT NULL:
    -- target_start_date = start_date
    -- IF start_date IS NULL:
    -- target_start_date = MIN(first_open_date of its children)
    WITH start_date_exist AS (
        SELECT
            issue_key,
            start_date AS target_start_date
        FROM `{{ curated.UNIQUE_ISSUES }}`
        WHERE start_date IS NOT NULL
    )

    SELECT *
    FROM start_date_exist
    UNION ALL
    SELECT
        parent_key,
        MIN(first_open_date) AS target_start_date
    FROM `{{ curated.UNIQUE_ISSUES }}`
    WHERE
        parent_key NOT IN (SELECT issue_key FROM start_date_exist)
        AND parent_key != "[UNKNOWN]"
    GROUP BY parent_key
) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN UPDATE SET
    ui.target_start_date = merge_source.target_start_date;
