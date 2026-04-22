from enum import Enum
from typing import Final

from airflow import settings

DATA_TRANSFER_AUDIT_TABLE_NAME: Final = 'DATA_TRANSFER_AUDIT'
DATA_TRANSFER_AUDIT_TABLE_DDL: Final = f'{settings.DAGS_FOLDER}/data_transfer/sql/create_data_transfer_audit_table.sql'
CTERA_SHARED_FOLDER: Final = 'ctera_shared_folder'
SHARED_DRIVE_CONFIG_FILE_PATH: Final = f'{settings.DAGS_FOLDER}/data_transfer/shared_drive_data_transfer/config/shared_drive_config.yaml'
TRANSFERRED: Final = '.transferred'


class DataTransferExecutionType(str, Enum):
    SHARED_DRIVE_DATA_EXPORT = 'Shared_Drive_Data_Export',
    SHARED_DRIVE_DATA_IMPORT = 'Shared_Drive_Data_Import',
    VENDOR_DATA_INBOUND = 'Vendor_Data_Inbound',
    VENDOR_DATA_OUTBOUND = 'Vendor_Data_Outbound'
