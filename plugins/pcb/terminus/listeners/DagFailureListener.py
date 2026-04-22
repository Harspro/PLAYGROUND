from __future__ import annotations
from airflow.listeners import hookimpl
from airflow.models.dagrun import DagRun
import logging
import json

logger = logging.getLogger(__name__)


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    dag = dag_run.get_dag()
    default_args = dag.default_args

    dag_details = {
        'dag_id': dag.dag_id,
        'dag_owner': default_args.get('owner'),
        'run_id': dag_run.run_id,
        'capability': default_args.get('capability'),
        'severity': default_args.get('severity'),
        'sub_capability': default_args.get('sub_capability'),
        'business_impact': default_args.get('business_impact'),
        'customer_impact': default_args.get('customer_impact')
    }

    dag_info = f"[DAG FAILED] Failed to run Airflow dag {json.dumps(dag_details)}"

    logger.critical(dag_info)
