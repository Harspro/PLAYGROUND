from dataclasses import dataclass
from typing import List


@dataclass
class RDB2BQChunkConfig:
    def __init__(self):
        pass

    partition_col: str
    chunk_size_in_months: int
    start_date: str
    end_date: str

    @staticmethod
    def from_dict(dict: dict):
        if dict is None:
            return None

        config = RDB2BQChunkConfig()
        config.partition_col = dict.get("partition_col")
        config.start_date = dict.get("start_date")
        config.end_date = dict.get("end_date")
        return config

    def to_dict(self) -> dict:
        c_dict = {"partition_col": self.partition_col,
                  "start_date": self.start_date,
                  "end_date": self.end_date
                  }
        return c_dict


@dataclass
class RDB2BQBQLoadConfig:
    def __init__(self):
        pass

    project_id: str
    dataset_id: str
    bq_table_name: str
    add_columns: List[str] = None
    drop_columns: List[str] = None
    add_file_name_col: bool = False
    bq_table_partition_field: str = None
    bq_table_cluster_fields: List[str] = None

    @staticmethod
    def from_dict(dict: dict):
        if dict is None:
            return None

        config = RDB2BQBQLoadConfig()
        config.project_id = dict.get("project_id")
        config.dataset_id = dict.get("dataset_id")
        config.add_columns = dict.get("add_columns")
        config.drop_columns = dict.get("drop_columns")
        config.add_file_name_col = dict.get("add_file_name_col")

        config.bq_table_name = dict.get("bq_table_name")
        config.bq_table_partition_field = dict.get("bq_table_partition_field")
        config.bq_table_cluster_fields = dict.get("bq_table_cluster_fields")
        return config

    def to_dict(self) -> dict:
        c_dict = {"project_id": self.project_id,
                  "dataset_id": self.dataset_id,
                  "add_file_name_col": self.add_file_name_col,
                  "bq_table_name": self.bq_table_name
                  }
        if self.add_columns:
            c_dict["add_columns"] = self.add_columns

        if self.drop_columns:
            c_dict["drop_columns"] = self.drop_columns

        if self.bq_table_partition_field:
            c_dict["bq_table_partition_field"] = self.bq_table_partition_field

        if self.bq_table_cluster_fields:
            c_dict["bq_table_cluster_fields"] = self.bq_table_cluster_fields

        return c_dict


@dataclass
class RDB2BQTableConfig:
    def __init__(self):
        pass

    ods_table_name: str
    drop_if_exists: bool
    primary_key: str
    read_parallelism: int

    bq_load_config: RDB2BQBQLoadConfig
    fetch_size: int = 500
    chunk_config: RDB2BQChunkConfig = None

    @staticmethod
    def from_dict(dict: dict):
        if dict is None:
            return None
        config = RDB2BQTableConfig()
        config.ods_table_name = dict.get("ods_table_name")
        config.primary_key = dict.get("primary_key")

        drop_if_exists_config = dict.get("drop_if_exists")
        config.drop_if_exists = True if drop_if_exists_config else False
        config.read_parallelism = dict.get("read_parallelism")
        config.chunk_config = RDB2BQChunkConfig.from_dict(dict.get("chunk_config"))
        config.bq_load_config = RDB2BQBQLoadConfig.from_dict(dict.get("bq_load_config"))
        config.fetch_size = dict.get("fetch_size") or 500
        return config

    def to_dict(self) -> dict:
        c_dict = {"ods_table_name": self.ods_table_name,
                  "primary_key": self.primary_key,
                  "drop_if_exists": self.drop_if_exists,
                  "read_parallelism": self.read_parallelism,
                  "fetch_size": self.fetch_size
                  }
        if self.chunk_config:
            c_dict["chunk_config"] = self.chunk_config.to_dict()
        if self.bq_load_config:
            c_dict["bq_load_config"] = self.bq_load_config.to_dict()

        return c_dict


@dataclass
class RDB2BQGCSConfig:
    def __init__(self):
        pass

    bucket: str
    staging_folder: str

    @staticmethod
    def from_dict(dict: dict):
        if dict is None:
            return None
        config = RDB2BQGCSConfig()
        config.bucket = dict.get("bucket")
        config.staging_folder = dict.get("staging_folder")
        return config

    def to_dict(self) -> dict:
        c_dict = {"bucket": self.bucket, "staging_folder": self.staging_folder}
        return c_dict


@dataclass
class RDB2BQSparkConfig:
    def __init__(self):
        pass

    jar_file_uris: List[str]
    file_uris: List[str]
    application_config: str
    properties: dict
    main_class: str = 'com.pcb.batch.odsbigqueryconnector.ODSToGCSConnector'

    @staticmethod
    def from_dict(dict: dict):
        if dict is None:
            return None

        config = RDB2BQSparkConfig()
        config.jar_file_uris = dict["jar_file_uris"]
        config.file_uris = dict["file_uris"]
        config.application_config = dict["application_config"]
        config.properties = dict.get("properties")
        config.main_class = dict["main_class"]
        return config


@dataclass
class RDB2BQDAGConfig:
    def __init__(self):
        pass

    read_pause_deploy_config: bool
    tags: str
    default_args: dict
    concurrency: int = 3
    max_active_runs: int = 1

    @staticmethod
    def from_dict(dict: dict):
        if dict is None:
            return None
        config = RDB2BQDAGConfig()
        config.read_pause_deploy_config = dict.get('read_pause_deploy_config')
        config.tags = dict.get("tags")
        config.default_args = dict.get("default_args")

        if dict.get("concurrency") is not None:
            config.concurrency = dict.get("concurrency")

        if dict.get("max_active_runs") is not None:
            config.max_active_runs = dict.get("max_active_runs")

        return config


@dataclass
class RDB2BQBulkLoaderConfig:
    def __init__(self):
        pass

    ods_db_name: str
    gcs: RDB2BQGCSConfig
    dag_config: RDB2BQDAGConfig
    spark_config: RDB2BQSparkConfig
    table_configs: List[RDB2BQTableConfig]
    ephemeral_cluster_name: str
    cluster_workers: int = 4
    num_concur_spark_jobs: int = 4

    @staticmethod
    def from_dict(dict: dict):
        if dict is None:
            return None

        config = RDB2BQBulkLoaderConfig()
        config.dag_config = RDB2BQDAGConfig.from_dict(dict.get("dag_config"))
        config.ods_db_name = dict.get('ods_db_name')
        config.cluster_workers = dict.get("cluster_workers") if dict.get("cluster_workers") else 4
        config.num_concur_spark_jobs = dict.get("num_concur_spark_jobs") if dict.get("num_concur_spark_jobs") else 4
        config.ephemeral_cluster_name = dict.get("ephemeral_cluster_name") or "ods-bulk-loader-ephemeral"
        config.spark_config = RDB2BQSparkConfig.from_dict(dict.get('spark_config'))
        config.gcs = RDB2BQGCSConfig.from_dict(dict.get("gcs"))
        table_config_dicts: List[dict] = dict.get('table_configs')
        if table_config_dicts:
            config.table_configs = [RDB2BQTableConfig.from_dict(t_dict) for t_dict in table_config_dicts]
        return config
