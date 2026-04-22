INSERT `{{ params.destination_table_name }}` (issue_key, event_time, from_{{ params.column_name }}, to_{{ params.column_name }})
WITH issues AS (SELECT issue_key,
                       created_date,
                       {{ params.column_name }}
                FROM `{{ curated.UNIQUE_ISSUES }}`
                {% if params.target_issue_types is string -%}
                WHERE issue_type = '{{ params.target_issue_types }}'
                {% else -%}
                WHERE issue_type IN {{ params.target_issue_types }}
                {% endif -%}),
     value_change AS (SELECT issue_key,
                             event_time,
                             {% if params.bq_data_type == "STRING" -%}
                             from_string AS from_{{ params.column_name }},
                             to_string   AS to_{{ params.column_name }}
                             {% elif params.bq_data_type == "FLOAT64" -%}
                             IFNULL(SAFE_CAST(from_string AS FLOAT64), 0) AS from_{{ params.column_name }},
                             IFNULL(SAFE_CAST(to_string AS FLOAT64), 0)   AS to_{{ params.column_name }}
                             {% endif -%}
                      FROM `{{ landing.CHANGELOG_EVENT }}`
                      WHERE field = '{{ params.clog_event_field }}'
                        AND field_type = '{{ params.clog_event_field_type }}'
                        AND field_id = '{{ params.clog_event_field_id }}'
                        AND issue_key IN (SELECT DISTINCT issue_key FROM issues)),
     creation_event AS (SELECT vc.issue_key,
                               i.created_date                  AS event_time,
                               MIN_BY(from_{{ params.column_name }}, event_time) AS to_{{ params.column_name }}
                        FROM value_change vc
                                 JOIN issues i
                                      ON vc.issue_key = i.issue_key
                        GROUP BY 1, 2
                        UNION ALL
                        SELECT issue_key,
                               created_date AS event_time,
                               {{ params.column_name }}       AS to_{{ params.column_name }}
                        FROM issues
                        WHERE issue_key NOT IN (SELECT DISTINCT issue_key FROM value_change))
SELECT issue_key,
       event_time,
       CAST(NULL AS {{ params.bq_data_type }}) AS from_{{ params.column_name }},
       to_{{ params.column_name }}
FROM creation_event
UNION ALL
SELECT *
FROM value_change;
