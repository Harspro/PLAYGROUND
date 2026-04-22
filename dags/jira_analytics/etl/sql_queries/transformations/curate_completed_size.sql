MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING (SELECT
    parent_key AS issue_key,
    SUM(CASE issue_type
        WHEN "epic" THEN mean_size
        WHEN "story" THEN story_point
    END) AS total_completed_size
FROM `{{ curated.UNIQUE_ISSUES }}`
WHERE
    issue_type IN ("epic", "story")
    AND resolution IN ("Done")
    AND parent_key != "[UNKNOWN]"
GROUP BY parent_key) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN UPDATE SET
    ui.total_completed_size = merge_source.total_completed_size;
