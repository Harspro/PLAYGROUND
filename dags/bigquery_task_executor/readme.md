# README: BigQueryTaskExecutor DAG Creator

## Overview
The `BigQueryTaskExecutor` class is designed to generate and manage Airflow DAGs for executing BigQuery staging queries based on a YAML configuration file. This provides an automated and scalable approach to running BigQuery jobs in an Airflow-managed workflow.

## Features
- Reads and processes configuration from YAML files.
- Generates DAGs dynamically with sequential task dependencies.
- Supports both inline SQL queries and external query files.
- Uses `BigQueryDbConnector` to execute queries.
- Integrates with Airflow’s scheduling and task execution framework.

## Dependencies
This script requires:
- **Airflow** (DAGs, Operators, Exceptions)
- **Pendulum** (Timezone management)
- **BigQueryDbConnector** (Custom BigQuery execution logic)
- **Utility Modules**:
  - `util.logging_utils` for failure logging
  - `util.miscutils` for file reading utilities
  - `util.constants` for configuration constants

## Configuration
The DAG configurations are stored in a YAML file, typically located in:
```
{settings.DAGS_FOLDER}/config/bigquery_task_executor_configs
```
Each DAG configuration should define:
- **DAG Metadata**: ID, schedule interval, description, timeout
- **BigQuery Tasks**: Queries, datasets, project IDs, replacements
- **Default Arguments**: Owner, severity, email notifications, etc.

## Usage
### Instantiating the DAG Creator
```python
executor = BigQueryTaskExecutor(config_filename='my_dag_config.yaml')
dags = executor.create_dags()
```
### Creating a DAG
The `create_dag` method constructs an Airflow DAG from a given configuration:
```python
dag = executor.create_dag(dag_id='example_dag', config=my_config_dict)
```
### Running BigQuery Tasks
The `_create_bigquery_tasks` method generates PythonOperators for executing queries:
```python
tasks = executor._create_bigquery_tasks(config=my_config_dict)
```

## Error Handling
- If a query or query file is missing, the DAG will raise an `AirflowFailException`.

## Example YAML Configuration
```yaml
example_dag:
  dag:
    schedule_interval: "30 05 * * *"
    description: 'Example DAG for BigQuery'
    dagrun_timeout: 120
    tags: ['bigquery', 'etl']
  bigquery_tasks:
    - task_id: 'run_query'
      query: 'SELECT * FROM my_dataset.my_table'
      project_id: 'my_project'
      dataset_id: 'my_dataset'
      table_id: 'my_table'
```

## License
This script is proprietary and intended for internal use.

## Contact
For support, reach out to the data engineering team.

Author: Yash Sharma