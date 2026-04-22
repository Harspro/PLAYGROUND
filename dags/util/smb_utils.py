from smbclient import open_file, register_session, reset_connection_cache, rmdir, listdir, scandir, mkdir, remove, rename
from smbclient.path import isdir
from google.cloud import storage
from abc import ABC
import util.constants as consts

import logging
logger = logging.getLogger(__name__)


class SMBUtil(ABC):
    def __init__(self, ctera_server_ip, ctera_username, ctera_password):
        self._server_ip = f'{ctera_server_ip}'
        self._username = f'{ctera_username}'
        self._password = f'{ctera_password}'

    # Copy Files From 'CTERA' TO 'GCS'
    def copy_ctera_to_gcs(self, ctera_folder, ctera_file_name, gcs_bucket, gcs_folder: None, gcs_file_name):
        connection_cache = {}

        try:
            source_file_path = f"//{self._server_ip}/{ctera_folder}/{ctera_file_name}"

            logger.info(f"filepath:{source_file_path}")

            if gcs_folder:
                gcs_path = f"{gcs_folder}/{gcs_file_name}"
            else:
                gcs_path = f"{gcs_file_name}"

            storage_client = storage.Client()
            bucket = storage_client.bucket(gcs_bucket)
            blob = bucket.blob(gcs_path)

            with open_file(source_file_path, mode='rb', username=self._username, password=self._password, connection_cache=connection_cache) as file_obj:
                logger.info("File copy started from CTERA to GCS")
                blob.upload_from_file(file_obj)                         # Default Chunk_Size : 40 MB
                logger.info(f"File is copied successfully in GCS bucket:gs://{gcs_bucket}/{gcs_path}")
                status = consts.CTERA_SUCCESS
                return status

        except Exception as err:
            status = consts.CTERA_FAIL
            logger.error(f"File copy from CTERA to GCS failed.Please check Error:{err}")
            return status
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # Copy Files From 'GCS' TO 'CTERA'
    def copy_gcs_to_ctera(self, ctera_folder, ctera_file_name, gcs_bucket, gcs_folder: None, gcs_file_name):
        connection_cache = {}
        try:
            source_file_path = f"//{self._server_ip}/{ctera_folder}/{ctera_file_name}"

            logger.info(f"filepath:{source_file_path}")

            if gcs_folder:
                gcs_path = f"{gcs_folder}/{gcs_file_name}"
            else:
                gcs_path = f"{gcs_file_name}"

            storage_client = storage.Client()
            bucket = storage_client.bucket(gcs_bucket)
            blob = bucket.blob(gcs_path)

            with open_file(source_file_path, mode="wb", username=self._username, password=self._password, connection_cache=connection_cache) as fd:
                logger.info(f"Blob's CHUNK_SIZE : {consts.BLOB_CHUNK_SIZE}")
                with blob.open("rb") as blob_reader:
                    while True:
                        chunk = blob_reader.read(consts.BLOB_CHUNK_SIZE)
                        if not chunk:
                            break
                        fd.write(chunk)
                logger.info(f"File is copied successfully in CTERA shared:{source_file_path}")
                status = consts.CTERA_SUCCESS
                return status

        except Exception as err:
            status = consts.CTERA_FAIL
            logger.error(f"File copy from GCS to CTERA failed.Please check Error:{err}")
            return status
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # Check the list of directory in CTERA folder
    def list_dir(self, ctera_dir_path: str):
        connection_cache = {}
        try:
            shared_dir_path = f"//{self._server_ip}/{ctera_dir_path}"
            return listdir(shared_dir_path, username=self._username, password=self._password, connection_cache=connection_cache)

        except Exception as err:
            logger.info(f"please check the error:{err}")
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # Check if given path is directory or not
    def is_dir(self, ctera_dir_path: str):
        connection_cache = {}
        try:
            shared_dir_path = f"//{self._server_ip}/{ctera_dir_path}"
            return isdir(shared_dir_path, username=self._username, password=self._password, connection_cache=connection_cache)
        except Exception as err:
            logger.info(f"please check the error:{err}")
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # List directories in given path along with their metadata
    def scan_dir(self, ctera_dir_path: str):
        connection_cache = {}
        try:
            shared_dir_path = f"//{self._server_ip}/{ctera_dir_path}"
            return scandir(shared_dir_path, username=self._username, password=self._password, connection_cache=connection_cache)

        except Exception as err:
            logger.info(f"please check the error:{err}")
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # Remove directory in given path
    def remove_dir(self, ctera_dir_path: str):
        connection_cache = {}
        try:
            shared_dir_path = f"//{self._server_ip}/{ctera_dir_path}"
            return rmdir(shared_dir_path, username=self._username, password=self._password, connection_cache=connection_cache)

        except Exception as err:
            logger.info(f"please check the error:{err}")
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # Create directory in given path
    def make_dir(self, ctera_dir_path: str):
        connection_cache = {}
        try:
            shared_dir_path = f"//{self._server_ip}/{ctera_dir_path}"
            return mkdir(shared_dir_path, username=self._username, password=self._password, connection_cache=connection_cache)

        except Exception as err:
            logger.info(f"please check the error:{err}")
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # Create directories recursively
    def make_dir_recursively(self, ctera_dir_path: str):
        if not self.is_dir(ctera_dir_path):
            directory_list = ctera_dir_path.strip('/').split('/')
            path = ''
            for dir in directory_list:
                path = f"{path}/{dir}"
                if not self.is_dir(path):
                    logger.info(f"creating dir {path}")
                    self.make_dir(path)

    # Delete a file
    def delete_file(self, ctera_file_path: str):
        connection_cache = {}
        try:
            shared_file_path = f"//{self._server_ip}/{ctera_file_path}"
            return remove(shared_file_path, username=self._username, password=self._password, connection_cache=connection_cache)

        except Exception as err:
            logger.info(f"please check the error:{err}")
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # Move or Rename a file
    def rename_file(self, ctera_old_file_path: str, ctera_new_file_path):
        connection_cache = {}
        try:
            share_old_filename = f"//{self._server_ip}/{ctera_old_file_path}"
            share_new_filename = f"//{self._server_ip}/{ctera_new_file_path}"
            return rename(src=share_old_filename, dst=share_new_filename, username=self._username, password=self._password, connection_cache=connection_cache)

        except Exception as err:
            logger.info(f"please check the error:{err}")
        finally:
            reset_connection_cache(connection_cache=connection_cache)

    # Read CTERA Shared File
    def read_file(self, ctera_file_path: str, ctera_file_name: str):
        connection_cache = {}
        try:
            ctera_file_full_path = f"//{self._server_ip}/{ctera_file_path}/{ctera_file_name}"
            with open_file(ctera_file_full_path, mode='rb', username=self._username, password=self._password, connection_cache=connection_cache) as file_obj:
                data = file_obj.readlines()
                return data
            file_obj.close()

        except Exception as err:
            logger.info(f"please check the error:{err}")
        finally:
            reset_connection_cache(connection_cache=connection_cache)
