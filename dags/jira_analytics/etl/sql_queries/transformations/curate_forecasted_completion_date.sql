MERGE INTO `{{ curated.UNIQUE_ISSUES }}` AS ui
USING
    (
        WITH initiative AS (
            SELECT
                issue_key,
                forecasted_burndown_rate
            FROM `{{ curated.UNIQUE_ISSUES }}`
            WHERE
                issue_type = "initiative"
                AND forecasted_burndown_rate IS NOT NULL
        ),

        epic AS (
            SELECT
                issue_key,
                parent_key
            FROM `{{ curated.UNIQUE_ISSUES }}`
            WHERE issue_type = "epic"
        ),

        current_dag_run_timestamp AS (
            SELECT
                TIMESTAMP(
                    DATETIME(TIMESTAMP(MAX(dag_run_time)), "America/Toronto")
                )
                    AS dag_run_time
            FROM `{{ landing.DAG_HISTORY }}`
        ),

        burndown AS (
            SELECT
                issue_key,
                event_time,
                burndown_weeks
            FROM `{{ curated.BURNDOWN_EVENT }}`
            WHERE issue_key IN (SELECT issue_key FROM epic)
        )

        SELECT
            init.issue_key,
            TIMESTAMP_ADD(
                cdrt.dag_run_time,
                INTERVAL CAST(
                    CEILING(
                        SUM(burn.burndown_weeks) / init.forecasted_burndown_rate
                    ) AS INT64
                )
                DAY
            ) AS forecasted_completion_date
        FROM initiative AS init
        INNER JOIN epic
            ON init.issue_key = epic.parent_key
        INNER JOIN burndown AS burn
            ON epic.issue_key = burn.issue_key,
            current_dag_run_timestamp AS cdrt
        GROUP BY
            init.issue_key, init.forecasted_burndown_rate, cdrt.dag_run_time
        HAVING SUM(burn.burndown_weeks) > 0
    ) AS merge_source
    ON ui.issue_key = merge_source.issue_key
WHEN MATCHED THEN UPDATE SET
    ui.forecasted_completion_date = merge_source.forecasted_completion_date;
