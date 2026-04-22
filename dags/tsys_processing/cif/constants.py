from typing import Final

from airflow import settings

CIF_CONFIG_FILE_NAME: Final[str] = "tsys_cif_file_ingestion_config.yaml"
CIF_CONFIG_FILE_PATH: Final[str] = f'{settings.DAGS_FOLDER}/tsys_processing/cif/config'
CIF_CARD_XREF_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/tsys_processing/cif/sql/cif_card_xref.sql'
MULTI_CARD_UNION_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/tsys_processing/cif/sql/cif_multi_card_records.sql'
CIF_ONE_CURRENT_CARD_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/tsys_processing/cif/sql/cif_one_current_card.sql'
CIF_MERGE_TRANSFORM_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/tsys_processing/cif/sql/cif_merge_transform.sql'
CIF_SEGMENT_PREV_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/tsys_processing/cif/sql/cif_segment_prev_loading.sql'
ACCOUNT_SEGMENT: Final = 'ACCOUNT'
CARD_SEGMENT: Final = 'CARD'
PREV_SUFFIX: Final = '_PREV'
