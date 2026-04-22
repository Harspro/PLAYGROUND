import json
import sys
import pendulum
import logging

from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    read_file_env,
    save_job_to_control_table,
    get_cluster_name_for_dag,
    get_cluster_config_by_job_size
)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.logging_utils import build_spark_logging_info
from util.bq_utils import apply_timestamp_transformation, table_exists, schema_preserving_load
import util.constants as consts
from dag_factory.terminus_dag_factory import add_tags

"""
This script is going to be deprecated. Please don't add more file loading into this script.
Please extend TsysFileLoader in tsys_file_loader_base.py
"""

DAG_DEFAULT_ARGS = {
    "owner": "team-centaurs",
    'capability': 'risk-management',
    'severity': 'P3',
    'sub_capability': 'fraud',
    'business_impact': 'Impact on FFM scoring engine',
    'customer_impact': 'N/A',
    "start_date": pendulum.today('America/Toronto').add(days=-1),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 10,
    "retries": 3,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True
}

default_owner = "team-centaurs"

is_ephemeral = True


def get_delta_dirs(bucket_name, **context):
    storage_client = storage.Client()
    folder_name_prefix = context['dag_run'].conf.get('folder_name') + '/'
    outputpath = context['dag_run'].conf.get('name') + '_extract/'

    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name_prefix, delimiter='/')

    print("Blobs:")
    for blob in blobs:
        print(blob.name)

    # for prefix in blobs.prefixes:
    #     print(prefix)

    dirnames = list(blobs.prefixes)
    dirnames.sort(reverse=True)
    print('\n'.join(dirnames))

    try:
        cur_idx = dirnames.index(outputpath)
    except ValueError:
        print("Outputpath not found in gcs")
        print(f"outputpath: {outputpath}, bucket_name: {bucket_name}, folder_name_prefix: {folder_name_prefix}")
        raise AirflowFailException("outputpath not found in the extract gcs bucket")

    if cur_idx + 1 <= len(dirnames) - 1:
        prior_idx = cur_idx + 1
    else:
        prior_idx = cur_idx

    print('Indexes: ', cur_idx, prior_idx)
    print(dirnames[cur_idx], dirnames[prior_idx])
    context['ti'].xcom_push(key="current_path", value=dirnames[cur_idx])
    context['ti'].xcom_push(key="prior_path", value=dirnames[prior_idx])


def extract_tables_info(dag_config: dict) -> list:
    """
    Extract table information from tsys_inbound_dags config.
    Handles ALL table naming patterns including layouts_to_bq_table overrides.
    """
    tables_info = []

    if not isinstance(dag_config, dict) or 'bigquery' not in dag_config:
        return tables_info

    bq_config = dag_config['bigquery']
    dataset_id = bq_config.get('dataset_id')
    project_id = bq_config.get('project_id')

    if not dataset_id:
        return tables_info

    # Handle TRLR section (single segment)
    if "TRLR" in dag_config:
        for segarg in dag_config["TRLR"]["segment_args"]:
            layoutname = segarg.get('pcb.tsys.processor.layout.name', '')
            bq_table_id = bq_config.get('table_name') or (bq_config.get('ext_table_layout_prefix') + f"_{layoutname}")

            # Apply the same override logic as in the main code
            if layoutname and 'layouts_to_bq_table' in bq_config and layoutname in bq_config['layouts_to_bq_table']:
                final_table_name = bq_config['layouts_to_bq_table'][layoutname]
            else:
                final_table_name = bq_table_id

            tables_info.append({
                'project_id': project_id,
                'dataset_id': dataset_id,
                'table_name': final_table_name
            })

    # Handle regular segments (spark_parse_job)
    if 'spark_parse_job' in dag_config and 'segment_args' in dag_config['spark_parse_job']:
        for segarg in dag_config['spark_parse_job']['segment_args']:
            layoutname = segarg.get('pcb.tsys.processor.layout.name', '')
            bq_table_id = bq_config.get('table_name') or (bq_config.get('ext_table_layout_prefix') + f"_{layoutname}")

            # Apply the same override logic as in the main code
            if layoutname and 'layouts_to_bq_table' in bq_config and layoutname in bq_config['layouts_to_bq_table']:
                final_table_name = bq_config['layouts_to_bq_table'][layoutname]
            else:
                final_table_name = bq_table_id

            tables_info.append({
                'project_id': project_id,
                'dataset_id': dataset_id,
                'table_name': final_table_name
            })

    # Handle direct table_name (simple single table DAGs)
    if not tables_info and 'table_name' in bq_config:
        tables_info.append({
            'project_id': project_id,
            'dataset_id': dataset_id,
            'table_name': bq_config['table_name']
        })

    return tables_info


def build_save_dag_to_bq_task(outputpath: str, **context):
    upstream_task_ids = list(context['ti'].task.upstream_task_ids)
    task_states = [context['dag_run'].get_task_instance(i).state for i in upstream_task_ids]
    tasks_success = [t[0] for t in zip(upstream_task_ids, task_states) if t[1] == 'success']
    tasks_failure = set(upstream_task_ids) - set(tasks_success)

    sidx = tasks_success[0].find("AM")
    segments_processed = [t[sidx:] for t in tasks_success]
    segments_failure = [t[sidx:] for t in tasks_failure]
    print(f'seg processed: {segments_processed}')
    context['ti'].xcom_push(key="segments_processed", value=segments_processed)
    context['ti'].xcom_push(key="segments_failure", value=segments_failure)

    job_params_str = json.dumps(
        {
            'source_filename': context['dag_run'].conf['name'],
            'extract_path': outputpath,
            'segments_processed': segments_processed,
            'segments_failure': segments_failure
        }
    )

    # Save job to control table (tables_info is pre-computed in op_kwargs)
    save_job_to_control_table(job_params_str, **context)
    if tasks_failure:
        print(f'tasks_failure: {tasks_failure}\n')
        raise AirflowFailException("Not all segments succeeded")


def get_file_create_dt(dag_id: str, dag_config: dict, **context):
    bigquery_client = bigquery.Client()
    bigquery_config = dag_config.get(consts.BIGQUERY)
    bq_processing_project_name = bigquery_config.get(consts.PROJECT_ID)
    bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
    file_date_column = bigquery_config.get("file_date_column")
    bq_table = bigquery_config.get("bigquery_table_name")

    create_date_sql = f"""
        SELECT MAX({file_date_column}) AS {file_date_column}
        FROM {bq_processing_project_name}.{bq_dataset_name}.{bq_table}
    """
    logging.info(f"Extracting file_create_dt using SQL: {create_date_sql}")
    create_dt_result = bigquery_client.query(create_date_sql).result().to_dataframe()
    file_create_dt = create_dt_result[file_date_column].values[0]

    if file_create_dt:
        logging.info(f"File create date for this execution is {file_create_dt}")
        context['ti'].xcom_push(key='file_create_dt', value=str(file_create_dt))
    else:
        raise AirflowException(f"file_create_dt is not found for {dag_id}")


# The list of tasks which are to be executed , and task level dependencies
def create_tasks(dag_config: dict, dag_id: str, cluster_name: str, job_size: str):
    done = EmptyOperator(task_id="Done", trigger_rule='none_failed')
    name = dag_id

    if 'join_segments' in dag_config['bigquery']:
        wait_for_all_segs_task = EmptyOperator(task_id="wait_for_all_segments")
    else:
        wait_for_all_segs_task = None

    # File-level processing first
    move_from_landing_to_processing = GCSToGCSOperator(
        task_id=f"moving_{name}_to_processed",
        gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
        source_bucket="{{ dag_run.conf['bucket'] }}",
        source_object="{{ dag_run.conf['name'] }}",
        destination_bucket=dag_config.get("processing_bucket"),
        destination_object="{{ dag_run.conf['name']}}",
        # move_object=True,
    )
    last_task = move_from_landing_to_processing

    # Set up job history task irrespective of any failures
    outputpath = f"gs://{dag_config.get('processing_bucket_extract')}/{{{{ dag_run.conf['name'] }}}}" + '_extract'
    save_dag_to_bq_task = PythonOperator(
        task_id='save_job_to_control_table',
        python_callable=build_save_dag_to_bq_task,
        op_kwargs={"outputpath": outputpath, "tables_info": extract_tables_info(dag_config)},
        retries=0,
        trigger_rule='none_failed'
    )

    if is_ephemeral:
        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=gcp_config.get("processing_zone_project_id"),
            cluster_config=get_cluster_config_by_job_size(deploy_env, gcp_config.get("network_tag"), job_size),
            region="northamerica-northeast1",
            cluster_name=cluster_name
        )
        last_task >> create_cluster
        last_task = create_cluster

    # Filtering job would go here
    if "spark_filter_job" in dag_config:
        filter_task = create_filtering_task(dag_config, cluster_name)
        last_task >> filter_task
        last_task = filter_task

    if "TRLR" in dag_config:
        for segarg in dag_config["TRLR"]["segment_args"]:
            layoutname = segarg.get('pcb.tsys.processor.layout.name', '')
            segmentname = segarg.get('pcb.tsys.processor.segment.name', '')
            duplicate_check_cols = segarg.get("pcb.tsys.processor.duplicate.columns", "")
            datafilepath = f"gs://{dag_config.get('processing_bucket')}/{{{{ dag_run.conf['name']}}}}"
            outputpath = f"gs://{dag_config.get('processing_bucket_extract')}/{{{{ dag_run.conf['name'] }}}}" + '_extract'
        layoutname_suffix = f"_{layoutname}"
        bq_config = dag_config["bigquery"]
        bq_table_id = bq_config.get('table_name') or (bq_config.get('ext_table_layout_prefix') + f"_{layoutname}")
        parquetpath = f"{outputpath}/{layoutname}"
        staging_table_ref = f"{gcp_config.get('processing_zone_project_id')}.{bq_config.get('dataset_id')}.{bq_table_id}_EXT"
        dest_table = f"{bq_config.get('project_id')}.{bq_config.get('dataset_id')}.{bq_table_id}"
        join_table_ref = f"{gcp_config.get('processing_zone_project_id')}.{bq_config.get('dataset_id')}.{bq_table_id}_JOIN"

        with TaskGroup("trailer_processing_group") as trailer_processing_group:
            parsing_task = trailer_parsing_task(dag_config, cluster_name, segarg, datafilepath, outputpath, layoutname_suffix, layoutname)
            leading_task = parsing_task
            external_table_task = load_data_into_bigquery(dag_config, name, segmentname, parquetpath, layoutname, wait_for_all_segs_task, leading_task, is_trailer=True)
            leading_task = external_table_task
            view_creation_task = join_segments_bigquery_table(dag_config, staging_table_ref, join_table_ref, layoutname, 'processing', leading_task, is_trailer=True)
            file_check_task = PythonOperator(
                task_id='duplicate_file_check_task',
                python_callable=duplicate_file_check,
                op_kwargs={
                    "staging_table_ref": staging_table_ref,
                    "dest_table": dest_table,
                    "duplicate_check_cols": duplicate_check_cols,
                    "segmentname": segmentname}
            )
            leading_task = file_check_task
            trailer_task = create_dest_bigquery_table(dag_config, join_table_ref, dest_table, layoutname, 'landing', leading_task, is_trailer=True)
            parsing_task >> external_table_task >> view_creation_task >> file_check_task >> trailer_task

        last_task >> trailer_processing_group
        last_task = trailer_processing_group

    if "spark_validation_job" in dag_config:
        for segarg in dag_config["spark_validation_job"]["segment_args"]:
            layout_name = segarg.get('pcb.tsys.processor.layout.name', '')
            segment_name = segarg.get('pcb.tsys.processor.segment.name', '')
            datafile_path = f"gs://{dag_config.get('processing_bucket')}/{{{{ dag_run.conf['name']}}}}"
            output_path = f"gs://{dag_config.get('processing_bucket_extract')}/{{{{ dag_run.conf['name'] }}}}" + '_extract'
            validation_columns = segarg.get("pcb.tsys.processor.validation.columns", "")
            validation_task = create_validation_task(dag_config, cluster_name, segarg, output_path, datafile_path, segment_name, layout_name, validation_columns)
            last_task >> validation_task
            last_task = validation_task

    # Segment level parsing
    for segarg in dag_config["spark_parse_job"]["segment_args"]:
        leading_task = last_task
        layoutname = segarg.get('pcb.tsys.processor.layout.name', '')
        segmentname = segarg.get('pcb.tsys.processor.segment.name', '')
        datafilepath = f"gs://{dag_config.get('processing_bucket')}/{{{{ dag_run.conf['name']}}}}"
        outputpath = f"gs://{dag_config.get('processing_bucket_extract')}/{{{{ dag_run.conf['name'] }}}}" + '_extract'

        if layoutname:
            layoutname_suffix = f"_{layoutname}"
            outputpath = f"{outputpath}/{layoutname}"
        else:
            layoutname_suffix = ""

        parsing_task = create_parsing_task(dag_config, cluster_name, segarg, datafilepath, outputpath, layoutname_suffix)

        leading_task >> parsing_task
        leading_task = parsing_task

        leading_task = load_data_into_bigquery(dag_config, name, segmentname, outputpath, layoutname,
                                               wait_for_all_segs_task, leading_task, is_trailer=False)

        if consts.KAFKA_WRITER in dag_config:
            if segmentname in dag_config[consts.KAFKA_WRITER]:
                dag_trigger_task = TriggerDagRunOperator(
                    task_id=f"{consts.DAG_TRIGGER_TASK}_{segmentname}",
                    trigger_dag_id=dag_config[consts.KAFKA_WRITER][segmentname].get(consts.DAG_ID),
                    logical_date=datetime.now(local_tz) + timedelta(hours=12),
                    conf={
                        consts.BUCKET: dag_config.get(consts.PROCESSING_BUCKET_EXTRACT),
                        consts.NAME: "{{ dag_run.conf['name'] }}",
                        consts.FOLDER_NAME: "{{ dag_run.conf['folder_name'] }}",
                        consts.FILE_NAME: "{{ dag_run.conf['file_name'] }}",
                        consts.CLUSTER_NAME: cluster_name
                    },
                    wait_for_completion=False,
                    poke_interval=30
                )
                leading_task >> save_dag_to_bq_task >> dag_trigger_task >> done
            else:
                leading_task >> save_dag_to_bq_task >> done
        else:
            leading_task >> save_dag_to_bq_task >> done

        submit_delta_tasks(dag_config, cluster_name, segmentname, layoutname, parsing_task, done)

    if consts.ORACLE_WRITER in dag_config:
        get_file_create_date_task = PythonOperator(
            task_id='get_file_date',
            python_callable=get_file_create_dt,
            op_kwargs={'dag_id': dag_id, 'dag_config': dag_config},
        )
        oracle_dag_trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{dag_config.get(consts.ORACLE_WRITER).get('dag_id')}",
            trigger_dag_id=dag_config[consts.ORACLE_WRITER].get(consts.DAG_ID),
            conf={
                "file_create_dt": "{{ ti.xcom_pull(task_ids='get_file_date', key='file_create_dt') }}"
            },
            wait_for_completion=False
        )

        leading_task >> save_dag_to_bq_task >> get_file_create_date_task >> oracle_dag_trigger_task >> done
    else:
        leading_task >> save_dag_to_bq_task >> done

    return move_from_landing_to_processing


def create_filtering_task(dag_config: dict, cluster_name: str):
    SPARK_FILTER_JOB = {
        "reference": {"project_id": dataproc_config.get('project_id')},
        "placement": {"cluster_name": cluster_name},
        "spark_job": {
            "jar_file_uris": dag_config["spark_filter_job"]["jar_file_uris"],
            "main_class": dag_config["spark_filter_job"]["main_class"],
            "args": dag_config["spark_filter_job"]["segment_args"]
        },
    }
    datafilepath = f"gs://{dag_config.get('processing_bucket')}/{{{{ dag_run.conf['name']}}}}"
    SPARK_FILTER_JOB['spark_job']['args'][0]['pcb.tsys.processor.datafile.path'] = datafilepath

    arglist = []
    for k, v in SPARK_FILTER_JOB['spark_job']['args'][0].items():
        arglist.append(f'{k}={v}')

    arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=DAG_DEFAULT_ARGS, arg_list=arglist)
    SPARK_FILTER_JOB['spark_job']['args'] = arglist
    # print(f"{SPARK_FILTER_JOB['spark_job']['args']}")

    return DataprocSubmitJobOperator(
        task_id="dataproc_submit_filter_job",
        job=SPARK_FILTER_JOB,
        region=dataproc_config.get("location"),
        project_id=dataproc_config.get('project_id'),
        gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
    )


def trailer_parsing_task(dag_config: dict, cluster_name: str, segarg: dict, datafilepath: str, outputpath: str, layoutname_suffix: str, layoutname: str):
    outputpath = f"{outputpath}/{layoutname}"
    SPARK_PARSE_JOB = {
        "reference": {"project_id": dataproc_config.get('project_id')},
        "placement": {"cluster_name": cluster_name},
        "spark_job": {
            "jar_file_uris": dag_config["TRLR"]["jar_file_uris"],
            "main_class": dag_config["TRLR"]["main_class"],
            "args": {**dag_config["TRLR"]["common_args"], **segarg}
        },
    }
    SPARK_PARSE_JOB['spark_job']['args']['pcb.tsys.processor.datafile.path'] = datafilepath
    SPARK_PARSE_JOB['spark_job']['args']['pcb.tsys.processor.output.path'] = outputpath

    arglist = []
    for k, v in SPARK_PARSE_JOB['spark_job']['args'].items():
        arglist.append(f'{k}={v}')

    arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=DAG_DEFAULT_ARGS, arg_list=arglist)
    SPARK_PARSE_JOB['spark_job']['args'] = arglist

    return DataprocSubmitJobOperator(
        task_id=f"dataproc_submit{layoutname_suffix}",
        job=SPARK_PARSE_JOB,
        region=dataproc_config.get("location"),
        project_id=dataproc_config.get('project_id'),
        gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
        trigger_rule='all_success'
    )


def duplicate_file_check(staging_table_ref, dest_table, duplicate_check_cols: list, segmentname, **context):
    bq_client = bigquery.Client()

    file_create_dt_query = f"""
        SELECT FILE_CREATE_DT
        FROM {staging_table_ref} LIMIT 1
    """
    logging.info(file_create_dt_query)
    try:
        file_create_dt_result = bq_client.query(file_create_dt_query).result().to_dataframe()
        if not file_create_dt_result.empty:
            file_create_dt = file_create_dt_result['FILE_CREATE_DT'].values[0]
            logging.info(f"Extracted file_create_dt: {file_create_dt}")
            context['ti'].xcom_push(key='file_create_dt', value=str(file_create_dt))
        else:
            logging.warning("No FILE_CREATE_DT found in staging table")
    except Exception as e:
        logging.warning(f"Could not extract FILE_CREATE_DT: {e}")

    if table_exists(bq_client, dest_table):
        if duplicate_check_cols:
            join_clause = " and ".join(map(lambda x: f't.{x} = s.{x}', duplicate_check_cols))
            duplicate_check_cols_str = ','.join([f's.{x}' for x in duplicate_check_cols])
            query = f"""SELECT {duplicate_check_cols_str} from {staging_table_ref} s JOIN {dest_table} t ON {join_clause} limit 1"""
            results = bq_client.query(query).result().to_dataframe()
            res = results.to_string(index=False)
            if not results.empty:
                raise AirflowFailException(
                    f"Duplicate entry with these details {res} exists for {segmentname} in "
                    f"source : '{staging_table_ref}'"
                )


def create_parsing_task(dag_config: dict, cluster_name: str, segarg: dict, datafilepath: str, outputpath: str, layoutname_suffix: str):
    # print(f'segarg: {segarg}')
    SPARK_PARSE_JOB = {
        "reference": {"project_id": dataproc_config.get('project_id')},
        "placement": {"cluster_name": cluster_name},
        "spark_job": {
            "jar_file_uris": dag_config["spark_parse_job"]["jar_file_uris"],
            "main_class": dag_config["spark_parse_job"]["main_class"],
            "args": {**dag_config["spark_parse_job"]["common_args"], **segarg}
        },
    }

    # logging.info(f'log datafilepath: {datafilepath}, outputpath: {outputpath}')

    SPARK_PARSE_JOB['spark_job']['args']['pcb.tsys.processor.datafile.path'] = datafilepath
    SPARK_PARSE_JOB['spark_job']['args']['pcb.tsys.processor.output.path'] = outputpath

    arglist = []
    for k, v in SPARK_PARSE_JOB['spark_job']['args'].items():
        arglist.append(f'{k}={v}')

    arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=DAG_DEFAULT_ARGS, arg_list=arglist)
    SPARK_PARSE_JOB['spark_job']['args'] = arglist
    # print(f"{SPARK_PARSE_JOB['spark_job']['args']}")

    return DataprocSubmitJobOperator(
        task_id=f"dataproc_submit{layoutname_suffix}",
        job=SPARK_PARSE_JOB,
        region=dataproc_config.get("location"),
        project_id=dataproc_config.get('project_id'),
        gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
    )


def create_validation_task(dag_config: dict, cluster_name: str, segarg: dict, output_path: str, datafile_path: str, segment_name: str, layout_name: str, validation_columns: list):

    SPARK_VALIDATE_JOB = {
        "reference": {"project_id": dataproc_config.get('project_id')},
        "placement": {"cluster_name": cluster_name},
        "spark_job": {
            "jar_file_uris": dag_config["spark_validation_job"]["jar_file_uris"],
            "main_class": dag_config["spark_validation_job"]["main_class"],
            "args": {**dag_config["spark_validation_job"]["common_args"], **segarg}
        }
    }

    SPARK_VALIDATE_JOB['spark_job']['args']['pcb.tsys.processor.datafile.path'] = datafile_path
    SPARK_VALIDATE_JOB['spark_job']['args']['pcb.tsys.processor.output.path'] = output_path
    SPARK_VALIDATE_JOB['spark_job']['args']['pcb.tsys.processor.validation.columns'] = ",".join(validation_columns)

    arglist = []
    for k, v in SPARK_VALIDATE_JOB['spark_job']['args'].items():
        arglist.append(f'{k}={v}')
    arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=DAG_DEFAULT_ARGS, arg_list=arglist)
    SPARK_VALIDATE_JOB['spark_job']['args'] = arglist

    return DataprocSubmitJobOperator(
        task_id=f"dataproc_validate_{segment_name}",
        job=SPARK_VALIDATE_JOB,
        region=dataproc_config.get("location"),
        project_id=dataproc_config.get('project_id'),
        gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
    )


def load_data_into_bigquery(dag_config: dict, name: str, segmentname: str, outputpath: str, layoutname: str,
                            wait_task, leading_task, is_trailer):
    bq_config = dag_config['bigquery']

    bq_table_id = bq_config.get('table_name') or (bq_config.get('ext_table_layout_prefix') + f"_{layoutname}")
    staging_table_ref = (f"{gcp_config.get('processing_zone_project_id')}."
                         f"{bq_config.get('dataset_id')}."
                         f"{bq_table_id}_EXT")

    curated_project_id = gcp_config.get('curated_zone_project_id')
    curated_table_ref = None
    if "spark_delta_job" in dag_config and "bq_dataset_id" in dag_config["spark_delta_job"]:
        curated_dataset_id = dag_config["spark_delta_job"]["bq_dataset_id"]
        curated_table_ref = f"{curated_project_id}.{curated_dataset_id}.{bq_table_id}"

    landing_project_id = bq_config.get('project_id') or gcp_config.get('landing_zone_project_id')
    landing_dataset_id = bq_config.get('dataset_id')

    if layoutname and layoutname in bq_config['layouts_to_bq_table']:
        landing_table_id = bq_config['layouts_to_bq_table'][layoutname]
    else:
        landing_table_id = bq_table_id

    landing_table_ref = f"{landing_project_id}.{landing_dataset_id}.{landing_table_id}"

    # print(f"staging: {staging_table_ref}")
    CREATE_EXTERNAL_TABLE_DDL = f"""
            CREATE OR REPLACE EXTERNAL TABLE
                `{staging_table_ref}`
            OPTIONS (
                format = 'PARQUET',
                uris = ['{outputpath}/*.parquet']
            )
        """
    task_suffix = f"_{layoutname}" if layoutname else ""
    create_staging_external_table = BigQueryInsertJobOperator(
        task_id=f"bigquery_create_staging_ext_table{task_suffix}",
        configuration={
            "query": {
                "query": CREATE_EXTERNAL_TABLE_DDL,
                "useLegacySql": False
            }
        },
        location=gcp_config.get('bq_query_location')
    )
    # For File Validation Check, only external table is created for Trailer.
    if is_trailer:
        leading_task >> create_staging_external_table
        return create_staging_external_table

    if 'join_segments' in bq_config:
        leading_task >> create_staging_external_table >> wait_task
        leading_task = wait_task
    else:
        leading_task >> create_staging_external_table
        leading_task = create_staging_external_table

    join_table_ref = (f"{gcp_config.get('processing_zone_project_id')}."
                      f"{bq_config.get('dataset_id')}."
                      f"{bq_table_id}_JOIN")

    if (bq_config.get('layouts_to_bq_table') and layoutname in bq_config['layouts_to_bq_table']) or \
            layoutname == "":
        leading_task = join_segments_bigquery_table(dag_config, staging_table_ref, join_table_ref, layoutname,
                                                    'processing', leading_task, is_trailer=False)

        if 'spark_delta_job' in dag_config and name == 'TS2_acctscores':
            leading_task = create_dest_bigquery_table(dag_config, join_table_ref, curated_table_ref, layoutname,
                                                      'curated', leading_task, is_trailer=False)

        leading_task = create_dest_bigquery_table(dag_config, join_table_ref, landing_table_ref, layoutname, 'landing', leading_task, is_trailer=False)

    return leading_task


def join_segments_bigquery_table(dag_config: dict, staging_table_ref: str, join_table_ref: str, layoutname: str,
                                 zone_name: str, leading_task, is_trailer):
    bq_config = dag_config['bigquery']
    join_sql_str = ""
    drop_columns = bq_config.get('drop_columns')
    drop_columns_str = ""
    drop_columns_str = ""
    if drop_columns:
        drop_columns_str = f"ALTER TABLE `{join_table_ref}` "
        drop_columns_str += ", ".join(["DROP COLUMN IF EXISTS " + c for c in drop_columns]) + ";"

    add_columns = bq_config.get('add_columns', "")
    add_columns_str = ""
    if add_columns:
        add_columns_str = ",\n".join(add_columns)

    if is_trailer:
        staging_table_ref_str = staging_table_ref
    else:
        if 'join_segments' in bq_config and layoutname in bq_config['join_segments']:
            join_sql_fname = bq_config['join_segments'][layoutname]
            join_sql_fname = f"{settings.DAGS_FOLDER}/tsys_processing/sql/{join_sql_fname}"
            join_sql_str = read_file_env(join_sql_fname, deploy_env)

        if join_sql_str:
            staging_table_ref_str = f"({join_sql_str})"
        else:
            staging_table_ref_str = staging_table_ref

    # print(f"\t{join_table_ref}")
    CREATE_JOIN_TABLE_DDL = f"""
        CREATE OR REPLACE VIEW
            `{join_table_ref}`
        OPTIONS (
          expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
          description = "Staging temp table for joining segments",
          labels = [("name", "join-table-tsys-inbound-dags")]
        )
        AS
            SELECT *, {add_columns_str}
            FROM {staging_table_ref_str};

        {drop_columns_str}
    """
    # print(CREATE_JOIN_TABLE_DDL)
    task_suffix = f"_{layoutname}" if layoutname else ""
    create_join_table = BigQueryInsertJobOperator(
        task_id=f"bigquery_create_join_table{task_suffix}",
        configuration={
            "query": {
                "query": CREATE_JOIN_TABLE_DDL,
                "useLegacySql": False
            }
        },
        location=gcp_config.get('bq_query_location')
    )
    leading_task >> create_join_table
    leading_task = create_join_table

    return leading_task


def archive_previous_bigquery_table(table_ref: str, layoutname: str, zone_name: str):
    CREATE_COPY_TABLE_DDL = f"""
                BEGIN
                    IF EXISTS(SELECT 1 from `{table_ref}` LIMIT 1) THEN
                        CREATE OR REPLACE TABLE
                            `{table_ref}_PREVIOUS` AS
                        SELECT
                            *
                        FROM `{table_ref}` t;
                    END IF;
                EXCEPTION WHEN ERROR THEN
                    SELECT @@error.message;
                END;
            """

    task_suffix = table_ref[table_ref.rfind(".") + 1:]
    task_id = f"bigquery_archive_{zone_name}_table_{task_suffix}"
    return BigQueryInsertJobOperator(
        task_id=f"{task_id}",
        configuration={
            "query": {
                "query": CREATE_COPY_TABLE_DDL,
                "useLegacySql": False
            }
        },
        location=gcp_config.get('bq_query_location')
    )


def create_dest_bigquery_table(dag_config: dict, join_table_ref: str, dest_table_ref: str, layoutname: str,
                               zone_name: str, leading_task, is_trailer):
    bq_config = dag_config['bigquery']
    task_suffix = dest_table_ref[dest_table_ref.rfind(".") + 1:]
    data_load_type = bq_config.get('data_load_type')
    partition_field = bq_config.get('partition_field')
    clustering_fields = bq_config.get('clustering_fields')

    # Conditional check to apply partitioning and clustering only for non-TRLR layouts
    partition_str = ""
    clustering_str = ""

    if not is_trailer:  # Skip partitioning and clustering for TRLR layout
        if partition_field:
            partition_str = f"PARTITION BY {partition_field}"

        if clustering_fields:
            clustering_str = f"CLUSTER BY {', '.join(clustering_fields)}"

    query_config = {
        'dest_table_ref': dest_table_ref,
        'join_table_ref': join_table_ref,
        'partition_str': partition_str,
        'clustering_str': clustering_str
    }

    # To validate the file ingestion, FILE_CREATE_DT is used from Trailer. So, APPEND_ONLY operation is used in this case.
    if is_trailer:
        data_load_type = "APPEND_ONLY"
    else:
        if 'keep_previous_table' in bq_config and bq_config['keep_previous_table']:
            archive_previous_table = archive_previous_bigquery_table(dest_table_ref, layoutname, zone_name)
            leading_task >> archive_previous_table
            leading_task = archive_previous_table

    if data_load_type == "FULL_REFRESH":
        task_id = f"bigquery_full_refresh_{zone_name}_table_{task_suffix}"
    elif data_load_type == "APPEND_ONLY":
        task_id = f"bigquery_append_{zone_name}_table_{task_suffix}"

    create_dest_table = PythonOperator(
        task_id=task_id,
        python_callable=build_loading_job,
        op_kwargs={'query_config': query_config,
                   'data_load_type': data_load_type}
    )

    leading_task >> create_dest_table
    leading_task = create_dest_table

    return leading_task


def build_loading_job(query_config: dict, data_load_type: str):
    bq_client = bigquery.Client()

    dest_table_ref = query_config.get('dest_table_ref')
    partition_str = query_config.get('partition_str')
    clustering_str = query_config.get('clustering_str')
    join_table_ref = query_config.get('join_table_ref')

    transformed_view = apply_timestamp_transformation(bq_client, {consts.ID: join_table_ref})
    join_table_ref = transformed_view.get(consts.ID)
    columns = transformed_view.get(consts.COLUMNS)

    if data_load_type == "FULL_REFRESH":
        create_ddl = f"""
                CREATE OR REPLACE TABLE
                `{dest_table_ref}`
                {partition_str}
                {clustering_str}
                AS
                SELECT {columns}
                FROM `{join_table_ref}`
            """
        insert_stmt = f"""
                    INSERT INTO `{dest_table_ref}`
                    SELECT {columns}
                    FROM `{join_table_ref}`
                """
        schema_preserving_load(create_ddl, insert_stmt, dest_table_ref, bq_client=bq_client)
    elif data_load_type == "APPEND_ONLY":
        dest_table_ddl = f"""
            CREATE TABLE IF NOT EXISTS
                `{dest_table_ref}`
                {partition_str}
                {clustering_str}
            AS
                SELECT *
                FROM `{join_table_ref}`
                LIMIT 0;

            INSERT INTO `{dest_table_ref}`
            SELECT {columns}
            FROM `{join_table_ref}`;
        """
        logging.info(dest_table_ddl)
        bq_client.query_and_wait(dest_table_ddl)
    else:
        logging.error('bigquery data_load_type not understood')
        raise AirflowFailException("config: bigquery data_load_type not understood")


def submit_delta_tasks(dag_config: dict, cluster_name: str, segmentname: str, layoutname: str, leading_task, next_task):
    # Segment level diff (delta)
    if "spark_delta_job" in dag_config \
            and segmentname in dag_config["spark_delta_job"]["segments_to_process"]:

        get_dirs = PythonOperator(
            task_id=f"get_delta_dirs_{segmentname}",
            python_callable=get_delta_dirs,
            op_kwargs={"bucket_name": dag_config.get("processing_bucket_extract")},
            retries=0
        )

        SPARK_DELTA_JOB = {
            "reference": {"project_id": dataproc_config.get('project_id')},
            "placement": {"cluster_name": cluster_name},
            "spark_job": {
                "jar_file_uris": dag_config["spark_delta_job"]["jar_file_uris"],
                "main_class": dag_config["spark_delta_job"]["main_class"],
                "file_uris": dag_config["spark_delta_job"]["file_uris"],
                "args": dag_config["spark_delta_job"]["segment_args"][0]
            }
        }

        file_path_base = f"gs://{dag_config.get('processing_bucket_extract')}/"
        current_path = f"{{{{ task_instance.xcom_pull(task_ids='get_delta_dirs_{segmentname}', key='current_path') }}}}"
        prior_path = f"{{{{ task_instance.xcom_pull(task_ids='get_delta_dirs_{segmentname}', key='prior_path') }}}}"

        SPARK_DELTA_JOB["spark_job"]["args"]["previousDayFilesPath"] = file_path_base + prior_path + layoutname
        SPARK_DELTA_JOB["spark_job"]["args"]["currentDayFilesPath"] = file_path_base + current_path + layoutname
        SPARK_DELTA_JOB["spark_job"]["args"]["segment"] = segmentname
        SPARK_DELTA_JOB["spark_job"]["args"]["applicationSuffix"] = dag_config["spark_delta_job"]["application_suffix"]

        if "TRLR" in dag_config:
            SPARK_DELTA_JOB["spark_job"]["args"]["fileCreateDt"] = "{{ ti.xcom_pull(task_ids='trailer_processing_group.duplicate_file_check_task', key='file_create_dt') }}"

        arglist = []
        for k, v in SPARK_DELTA_JOB['spark_job']['args'].items():
            arglist.append(f'{k}={v}')

        arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=DAG_DEFAULT_ARGS, arg_list=arglist)
        SPARK_DELTA_JOB['spark_job']['args'] = arglist
        # print(f"{SPARK_DELTA_JOB['spark_job']['args']}")

        submit_spark_delta_job = DataprocSubmitJobOperator(
            task_id=f"dataproc_submit_delta_{segmentname}",
            job=SPARK_DELTA_JOB,
            region=dataproc_config.get("location"),
            project_id=dataproc_config.get('project_id'),
            gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
            retries=1
        )

        (leading_task >> get_dirs >> submit_spark_delta_job >> next_task)


class DagBuilder:

    def __init__(self, config: dict):
        self.config = config

    @classmethod
    def from_json_string(cls, config_json: str) -> 'DagBuilder':
        config = json.loads(config_json)
        return cls(config)

    @classmethod
    def from_json(cls, config_json: dict) -> 'DagBuilder':
        config = config_json
        return cls(config)

    def create_def_args(self, dag_config: dict):
        default_args = DAG_DEFAULT_ARGS.copy()
        default_args.update({"owner": f"{dag_config.get('owner')}" if "owner" in dag_config else default_owner})
        return default_args

    def create_dag(self, dag_id: str, dag_config: dict, default_args: dict):
        tags = dag_config.get(consts.TAGS)
        dag = DAG(dag_id=dag_id,
                  start_date=datetime(2023, 1, 1, tzinfo=local_tz),
                  tags=tags,
                  schedule=None,
                  max_active_runs=1,
                  catchup=False,
                  is_paused_upon_creation=True,
                  default_args=default_args)

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, deploy_env)
                pause_unpause_dag(dag, is_paused)

            job_size = dag_config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)
            create_tasks(dag_config, dag_id, cluster_name, job_size)

        return add_tags(dag)

    def generate_dags(self) -> dict:
        dags = {}

        for key, values in self.config.items():
            dags[key] = self.create_dag(key, values, self.create_def_args(values))

        return dags


dataproc_config = read_variable_or_file("dataproc_config")
gcp_config = read_variable_or_file("gcp_config")
deploy_env = gcp_config['deployment_environment_name']
dags_config_fname = f"{settings.DAGS_FOLDER}/tsys_processing/tsys_inbound_dags_config.yaml"
dags_config = read_yamlfile_env(f"{dags_config_fname}", deploy_env)
local_tz = pendulum.timezone('America/Toronto')
if not dags_config:
    sys.exit('No tsys dags config file found')

dag_builder = DagBuilder.from_json(dags_config)
globals().update(dag_builder.generate_dags())
