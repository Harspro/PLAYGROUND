# JIRA ANALYTICS AIRFLOW GUIDE

## Development Requirements
- Python Version: 3.11.5
- Apache Airflow Version: 2.7.3

## Local Environment Setup Guide

### Create & Activate Virtual Environment
```shell
cd dags/jira_analytics
python3 -m venv venv
source venv/bin/activate
```

### Install Required Python Packages
```shell
cd dags/jira_analytics
pip install requirements.txt
```

## Airflow Guide
If you are not familiar with **Airflow**, please refer
to [Official Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/2.7.3/).

## Extracting New Field from JIRA Cloud
[PLACE HOLDER]

## Extracting New Changelog Field from JIRA Cloud
[PLACE HOLDER]

## Deploying Updates in SQL Files
There are some steps you must take before deploying the updates in SQL files.

### Lint SQL files using sqlfluff
Please run below commands to lint all .sql files before creating an MR. You **MUST** install 
[required packages](#install-required-python-packages) before running below commands.

#### Check Lint Errors in SQL
This command shows all lint errors in .sql files under `etl/sql_queries/` directory.

```shell
sqlfluff lint --show-lint-violations etl/sql_queries/
```

#### Fix Lint Errors in SQL
This command fixes all lint errors in .sql files under `etl/sql_queries/` directory.

```shell
sqlfluff fix show-lint-violations etl/sql_queries/
```




