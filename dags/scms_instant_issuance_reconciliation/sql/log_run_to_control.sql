INSERT INTO `{control_table_id}`
VALUES (
	'{dag_id}',
	'{start_time}',
	'{end_time}',
	'{dag_execution_time}',
	'{trigger_type}',
	CAST('{replacement_count}' AS INT)
	)

