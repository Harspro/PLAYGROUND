from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator
from airflow.utils.decorators import apply_defaults


class CustomGKEStartPodOperator(GKEStartPodOperator):

    @apply_defaults
    def __init__(self, env_vars=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.env_vars = env_vars

    def execute(self, context):
        # Store pdf path in xcom for job status retrieval
        # example input paths: manual__2025_07_14T19_06_25_00_00/LETTER/SIMPLEX/EN,
        #                      manual__2025_07_14T19_06_25_00_00/LETTER/DUPLEX_2/FR
        path = self.env_vars.get('INPUT_PDFS_PATH').split('/', 1)[1]

        context['task_instance'].xcom_push(key='path', value=path)
        context['task_instance'].xcom_push(key='outbound_filename',
                                           value=f"{self.env_vars.get('OUTPUT_FILE_NAME')}{self.env_vars.get('OUTPUT_FILE_EXTENSION')}")
        super().execute(context)
