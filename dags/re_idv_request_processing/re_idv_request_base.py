import logging
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import util.constants as consts
from datetime import datetime
from etl_framework.etl_dag_base import ETLDagBase

logger = logging.getLogger(__name__)


class ReIdvProcessor(ETLDagBase):
    def __init__(self, module_name, config_path, config_filename, dag_default_args):
        super().__init__(module_name, config_path, config_filename, dag_default_args)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def preprocessing_job(self, config: dict, upstream_task: list):
        pass

    def postprocessing_job(self, config: dict, upstream_task: list):
        task = TriggerDagRunOperator(
            task_id=config[consts.DAG][consts.TASK_ID].get(consts.KAFKA_WRITER_TASK_ID),
            trigger_dag_id=config[consts.DAG][consts.DAG_ID].get(consts.KAFKA_TRIGGER_DAG_ID),
            conf={
                'min_hours_between_reminders': "{{ dag_run.conf.get('min_hours_between_reminders') }}"
            },
            wait_for_completion=config[consts.DAG].get(consts.WAIT_FOR_COMPLETION),
            poke_interval=config[consts.DAG].get(consts.POKE_INTERVAL),
            logical_date=datetime.now(tz=self.local_tz)
        )
        upstream_task.append(task)
        return upstream_task
