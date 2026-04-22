import logging
import os
import tarfile
import tempfile
import pendulum
from datetime import timedelta
import util.constants as consts
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.empty import EmptyOperator
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import read_variable_or_file, read_yamlfile_env
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class GCSTarExtraction:
    """
    GCS Tar Extraction class to create DAGs dynamically.
    """

    def __init__(self, config_filename: str, config_dir: str = None):
        """
        Constructor: sets up GCP/job configuration for class.
        :param config_filename: configuration file name.
        :type config_filename: str

        :param config_dir: configuration directory name, if none is provided will utilize
            the settings.DAGS_FOLDER.
        :type config_dir: str
        """

        self.local_tz = pendulum.timezone('America/Toronto')

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/idl_consent_signature'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

    @staticmethod
    def safe_extract(tar, path):
        """
        Safely extract tar files by removing absolute paths and ensuring safe directories.
        """
        for member in tar.getmembers():
            if member.islnk() or member.issym():
                logger.warning(f"Skipping symbolic link: {member.name}")
                continue
            member.name = os.path.normpath(member.name).lstrip("/")
            safe_path = os.path.join(path, member.name)
            if not safe_path.startswith(path):
                logger.warning(f"Skipping potentially unsafe path: {member.name}")
                continue
            tar.extract(member, path=path)

    @staticmethod
    def extract_and_upload(**kwargs):
        """
        Extracts a .tar file from GCS and uploads extracted files back to GCS while preserving folder structure.
        """

        bucket_name = kwargs['bucket_name']
        source_blob = kwargs['source_blob']
        destination_bucket = kwargs['destination_bucket']
        destination_folder = kwargs['destination_folder']

        gcs_hook = GCSHook()
        temp_dir = tempfile.mkdtemp()
        tar_path = os.path.join(temp_dir, source_blob.split('/')[-1])
        gcs_hook.download(bucket_name, source_blob, tar_path)

        extract_path = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_path, exist_ok=True)
        with tarfile.open(tar_path, 'r') as tar:
            GCSTarExtraction.safe_extract(tar, extract_path)

        for root, _, files in os.walk(extract_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, extract_path)
                gcs_hook.upload(destination_bucket, f"{destination_folder}/{relative_path}", file_path)
        logger.info(f"Extraction completed for {source_blob}")

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """
        Creates a DAG for extracting a single tar file from GCS and storing files in a folder structure.
        """

        default_args = {
            'owner': config.get('default_args', {}).get('owner', 'airflow'),
            'start_date': pendulum.now().subtract(days=1),
            'retries': 1,
        }

        dag_timeout = config.get(consts.DAG, {}).get(consts.DAGRUN_TIMEOUT, 30)

        env = self.deploy_env
        source_bucket = config.get('source_bucket').format(env=env)
        destination_bucket = config.get('destination_bucket').format(env=env)
        tar_file = config.get('tar_file')

        # Validate tar_file and handle the case where it's None
        if not tar_file:
            raise ValueError("tar_file is required")
        task_id = f"extract_{tar_file.replace('.', '_')}" if tar_file else 'extract_&_upload'

        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description="Extract a tar file from GCS and upload files to another bucket while maintaining structure.",
            render_template_as_native_obj=True,
            schedule=None,
            catchup=False,
            max_active_runs=1,
            dagrun_timeout=timedelta(minutes=dag_timeout),
            is_paused_upon_creation=True,
            tags=config.get(consts.DAG, {}).get(consts.TAGS),
        )

        with dag:
            if config.get(consts.DAG, {}).get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start_task = EmptyOperator(task_id=consts.START_TASK_ID)
            end_task = EmptyOperator(task_id=consts.END_TASK_ID)

            extract_task = PythonOperator(
                task_id=task_id,
                python_callable=self.extract_and_upload,
                op_kwargs={
                    "bucket_name": source_bucket,
                    "source_blob": tar_file,
                    "destination_bucket": destination_bucket,
                    "destination_folder": config.get('destination_folder'),
                },
            )

            start_task >> extract_task >> end_task

        return add_tags(dag)

    def create_dags(self) -> dict:

        if self.job_config:
            dags = {}
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)
            return dags
        return {}
