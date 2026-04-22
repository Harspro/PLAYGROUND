MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING
    (
        WITH initiative AS (
            SELECT
                issue_key,
                target_start_date
            FROM `{{ curated.UNIQUE_ISSUES }}`
            WHERE
                issue_type = "initiative"
                AND target_start_date IS NOT NULL
                AND total_size > 0
                AND status NOT IN ("Done", "Closed")
        ),

        epic AS (
            SELECT
                issue_key,
                parent_key
            FROM `{{ curated.UNIQUE_ISSUES }}`
            WHERE issue_type = "epic"
        ),

        burndown AS (
            SELECT
                issue_key,
                event_time,
                burndown_weeks
            FROM `{{ curated.BURNDOWN_EVENT }}`
            WHERE
                event_type = "BURNDOWN"
                AND issue_key IN (SELECT issue_key FROM epic)
        )

        SELECT
            init.issue_key,
            -1 * SUM(burn.burndown_weeks)
            / TIMESTAMP_DIFF(
                MAX(burn.event_time), init.target_start_date, DAY
            ) AS forecasted_burndown_rate
        FROM initiative AS init
        INNER JOIN epic
            ON init.issue_key = epic.parent_key
        INNER JOIN burndown AS burn
            ON epic.issue_key = burn.issue_key
        WHERE burn.event_time >= init.target_start_date
        GROUP BY init.issue_key, init.target_start_date
        HAVING
            SUM(burn.burndown_weeks) != 0
            AND forecasted_burndown_rate != 0
    ) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN UPDATE SET
    ui.forecasted_burndown_rate = merge_source.forecasted_burndown_rate;
