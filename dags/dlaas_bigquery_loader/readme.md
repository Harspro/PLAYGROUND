# DLaaS Documentation

At the time of writing DLaaS supports onetime and chunk loading. Below I will provide templates of both and explain
what's happening in the background at a high level.

## Current Flow

User submits a JSON request via a UI. This JSON payload is passed to 'dlaas_bigquery_loader_trigger' cloud function that
resides in landing. This cloud function triggers the 'dlaas_request_saver' DAG. This DAG converts the JSON payload into
a YAML payload. The YAML payload is written to a config file located under 'data/dlaas_bigquery_loader_configs/onetime'
or 'data/dlaas_bigquery_loader_configs/chunk' depending on the type of load we are doing. The YAML config is picked up
by dlaas onetime/chunk launcher to create the corresponding DAG which runs immediately.

## Example JSON payloads

Below I will provide templates on how the JSON payload should be structured when submitting DLaaS requests.

### Onetime

```
{
  "type": "onetime",
  "conf": {
    "template_onetime_loading_job": {
      "args": {
        "pcb.bigquery.loader.source.schema.qualified.tablename": "SCHEMA.TEMPLATE_TABLE",
        "pcb.bigquery.loader.source.primary.key.column": "KEY",
        "pcb.bigquery.loader.source.db.read.parallelism": 16,
        "pcb.bigquery.loader.source.jdbc.url": "url",
        "pcb.bigquery.loader.source.jdbc.user": "user",
        "pcb.bigquery.loader.source.secret.path": "/path/to/secret/path",
        "pcb.bigquery.loader.source.db.password.key": "dbpass"
      },
      "gcs": {
        "bucket": "pcb-{env}-staging-extract",
        "staging_folder": "exampledb/onetime"
      },
      "bigquery": {
        "project_id": "pcb-{env}-landing",
        "dataset_id": "domain_example",
        "table_name": "TEMPLATE_TABLE"
      },
      "dag": {
        "fetching_task_id": "fetch_template_table",
        "loading_task_id": "load_template_table_to_bigquery"
      }
    }
  }
}
```

The corresponding YAML file that is generated and used to create the corresponding DAG is:

```
template_onetime_loading_job:
  args:
    pcb.bigquery.loader.source.schema.qualified.tablename: SCHEMA.TEMPLATE_TABLE
    pcb.bigquery.loader.source.primary.key.column: KEY
    pcb.bigquery.loader.source.db.read.parallelism: 16
    pcb.bigquery.loader.source.jdbc.url: url
    pcb.bigquery.loader.source.jdbc.user: user
    pcb.bigquery.loader.source.secret.path: /path/to/secret/path
    pcb.bigquery.loader.source.db.password.key: dbpass
  gcs:
    bucket: pcb-{env}-staging-extract
    staging_folder: exampledb/onetime
  bigquery:
    project_id: pcb-{env}-landing
    dataset_id: domain_example
    table_name: TEMPLATE_TABLE
  dag:
    fetching_task_id: fetch_template_table
    loading_task_id: load_template_table_to_bigquery
```

**Note:** The above YAML file is just present for visualization purposes

## Chunk

```
{
  "type": "chunk",
  "conf": {
    "template_chunk_loading_job": {
      "fetch_spark_job": {
        "args": {
          "pcb.bigquery.loader.source.schema.qualified.tablename": "SCHEMA.TEMPLATE_TABLE",
          "pcb.bigquery.loader.source.db.read.parallelism": 16,
          "pcb.bigquery.loader.source.partition.column": "pcol",
          "pcb.bigquery.loader.source.jdbc.url": "url",
          "pcb.bigquery.loader.source.jdbc.user": "user",
          "pcb.bigquery.loader.source.secret.path": "/path/to/secret/path",
          "pcb.bigquery.loader.source.db.password.key": "dbpass"
        }
      },
      "bigquery": {
        "project_id": "pcb-{env}-landing",
        "dataset_id": "domain_example",
        "table_name": "TEMPLATE_TABLE",
        "partition_field": "DATE_TRUNC(FILE_CREATE_DT, MONTH)",
        "clustering_fields": [
          "FIELD1"
        ],
        "add_columns": [
          "ADDCOL1"
        ],
        "drop_columns": [
          "DROPCOL1",
          "DROPCOL2"
        ],
        "join": {
          "table_name": "JOIN_TABLE",
          "output_columns": [
            "OUTPUT1"
          ],
          "join_columns": [
            "JOIN1"
          ],
          "filters": [
            {
              "column_name": "FILTER1",
              "column_value": "FILTER_COL_VAL"
            }
          ]
        }
      },
      "batch": {
        "chunk_size_in_months": 12,
        "start_date": "2016-09-01",
        "end_date": "2022-12-31"
      },
      "merge": {
        "read_pause_deploy_config": true,
        "chunk_size_in_months": 5,
        "start_date": "2016-04-01",
        "end_date": "2016-08-31",
        "join_columns": [
          "JOIN1",
          "JOIN2"
        ]
      }
    }
  }
}
```

The corresponding YAML file that is generated and used to create the corresponding DAG is:

```
template_chunk_loading_job:
  fetch_spark_job:
    args:
      pcb.bigquery.loader.source.schema.qualified.tablename: SCHEMA.TEMPLATE_TABLE
      pcb.bigquery.loader.source.db.read.parallelism: 16
      pcb.bigquery.loader.source.partition.column: pcol
      pcb.bigquery.loader.source.jdbc.url: url
      pcb.bigquery.loader.source.jdbc.user: user
      pcb.bigquery.loader.source.secret.path: /path/to/secret/path
      pcb.bigquery.loader.source.db.password.key: dbpass
  bigquery:
    project_id: pcb-{env}-landing
    dataset_id: domain_example
    table_name: TEMPLATE_TABLE
    partition_field: DATE_TRUNC(FILE_CREATE_DT, MONTH)
    clustering_fields:
      - FIELD1
    add_columns:
      - ADDCOL1
    drop_columns:
      - DROPCOL1
      - DROPCOL2
    join:
      table_name: JOIN_TABLE
      output_columns:
        - OUTPUT1
      join_columns:
        - JOIN1
      filters:
        - column_name: FILTER1
          column_value: FILTER_COL_VAL
  batch:
    chunk_size_in_months: 12
    start_date: '2016-09-01'
    end_date: '2022-12-31'
  merge:
    read_pause_deploy_config: true
    chunk_size_in_months: 5
    start_date: '2016-04-01'
    end_date: '2016-08-31'
    join_columns:
      - JOIN1
      - JOIN2
```

**Note:** The above YAML file is just present for visualization purposes

## Observations:

* DLaaS onetime YAML is identical to the current way we do onetime loading

* DLaaS chunk YAML is different. For DLaaS, we do not provide dbname. Furthermore, we provide db arguments as part of
  the arguments of the spark job. Rest of it works similarly to how we define the YAML currently for chunk loading

* There is a 64 character limit for service account name length in Airflow. This is referring to service accounts in the
  form of "<example_account>@${var.landing_project_id}.iam.gserviceaccount.com". The total length of the aforementioned
  string should be up to 64 characters
