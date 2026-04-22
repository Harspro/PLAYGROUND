"""
This class defines methods related to validating whether a file exists for file loading pathways.
"""
import logging
from datetime import datetime, timedelta
import pandas as pd
import pytz
from airflow.exceptions import AirflowException
from google.cloud import bigquery

from data_validation.base_validator import BaseValidator
from data_validation.utils.utils import check_holiday
from util.constants import HOURS_DELTA, DAYS_DELTA, YYYY_MM_DD, YYYY_MM_DD_HH


class DagValidator(BaseValidator):
    def get_latest_dag_run_timestamp(self, dag_id: str) -> pd.Timestamp:
        bigquery_client = bigquery.Client()
        query = f"""
            SELECT DATETIME(MAX(load_timestamp), 'America/Toronto') as MAX_LOAD_TIMESTAMP
            FROM `pcb-{self.deploy_env}-curated.JobControlDataset.DAGS_HISTORY`
            WHERE dag_id = '{dag_id}'
        """
        logging.info(f"[QUERY] {query}")
        tms_df = bigquery_client.query(query).result().to_dataframe()
        last_run = pd.to_datetime(tms_df['MAX_LOAD_TIMESTAMP'].values[0])
        if pd.isnull(last_run):
            raise AirflowException(f"[ERROR] No previous DAG run found for dag_id='{dag_id}'.")
        return last_run

    def validate(self, config: dict) -> None:
        vendor = config.get('vendor')
        dag_id = config.get("dag_id")
        last_run = self.get_latest_dag_run_timestamp(dag_id)
        last_run_date_tz = last_run.strftime(YYYY_MM_DD)
        last_run_hour_tz = last_run.strftime(YYYY_MM_DD_HH)
        current_run_date_tz = datetime.now().astimezone(pytz.timezone("America/Toronto"))
        logging.info(f"last_run_date_tz: {last_run_date_tz}, last_run_hour_tz: {last_run_hour_tz}")

        current_run_list = []
        days_delta = config.get(DAYS_DELTA)
        hours_delta = config.get(HOURS_DELTA)

        if days_delta:  # excluding current date
            for i in range(1, days_delta + 1):
                current_run_list.append(
                    ((current_run_date_tz - timedelta(days=i)).strftime(YYYY_MM_DD)))
        elif hours_delta:
            for i in range(hours_delta, -1, -1):  # including current hour
                current_run_list.append(
                    ((current_run_date_tz - timedelta(hours=i)).strftime(YYYY_MM_DD_HH)))
        elif current_run_date_tz:  # including current date
            current_run_list.append(current_run_date_tz.strftime(YYYY_MM_DD))

        logging.info(f"current list to check the file load date \n {current_run_list}")

        if (last_run_date_tz in current_run_list) or (last_run_hour_tz in current_run_list):
            logging.info(f"Dag '{dag_id}' ran successfully")
        else:
            is_holiday = check_holiday(vendor, current_run_date_tz, hours_delta)
            if is_holiday:
                logging.info(
                    f"Dag '{dag_id}' is not triggered due to {vendor} holiday")
                return
            else:
                raise AirflowException(
                    f"[ERROR] Dag '{dag_id}' has not been triggered today [{current_run_date_tz.strftime('%Y-%m-%d %H:%M:%S')}] in EST.")
