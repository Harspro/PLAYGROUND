"""
This file defines the PCBSFTPOperator class.
"""

from abc import ABC
from io import StringIO
from typing import Sequence, Any, Union, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.settings import DAGS_FOLDER
from paramiko import RSAKey

from pcf_operators.sftp_operator.model.sftp_server_config import SftpServerConfig
from util.constants import GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME
from util.miscutils import read_variable_or_file, read_yamlfile_env
from util.vault_util import VaultUtilBuilder


class PCBSFTPOperator(BaseOperator, ABC):
    """
    This class defines the base SFTP Operator that handles the transfer of files between the
    drive and a remote server via secured file transfer protocol (SFTP).

    Please note that this operator does not have an implemented `execute` method:
        - For getting file from a remote server, please use the PCBSFTPGetOperator.
        - For sending file to a remote server, please use PCBSFTPPutOperator.
    """

    # This is the local filepath to grab the file from or save the file to. Please be aware
    # that the filepath is relative to the airflow directory. For example, if you want to
    # move a file called EXAMPLE.CSV that exists in the "data" folder in the airflow directory
    # then you will need to specify 'local_filepath' as /home/airflow/gcs/data/EXAMPLE.csv".
    # This field is templated.
    local_filepath: str

    # This is the remote filepath where you want to put the file or pull the file from. For
    # example if you want to place a file called "EXAMPLE.csv" in a folder called "sftp_test"
    # in the remote server, you should specify the 'remote_filepath' argument as
    # sftp_test/EXAMPLE.csv. This field is templated.
    remote_path: str

    # This is a Boolean indicating whether intermediate directories should be created on the
    # remote host if they are missing. This field is templated.
    create_intermediate_dirs: bool

    # This the key to look up in the config file to get all the information required to make the
    # connection. For example, in pcb-dp-data-fraud-airflow-dag/dags/config/sftp_dags_config.yaml
    # 'lookup_key' can also be a Sequence for example: ['key1', 'key2', 'key3'] or
    # ('key1', 'key2', 'key3). This can be useful if there are multiple layers in the config file.
    # Defaults to None. This field is templated.
    lookup_key: Union[str, Sequence[str]]

    # File path for the config file which contains the details to connect to the remote host. The
    # config file has to adhere to a certain format. Please refer to dags/config/sftp_dags.yaml
    # for the format. Defaults to None. This field is templated.
    config_path: str

    template_fields: Sequence[str] = ("local_filepath", "remote_filepath", "create_intermediate_dirs",
                                      "lookup_key", "config_path")

    def __init__(self,
                 local_filepath: str,
                 remote_filepath: str,
                 create_intermediate_dirs: bool = False,
                 lookup_key: Union[str, Sequence[str]] = (),
                 config_path: Optional[str] = None,
                 **kwargs):
        """
        This function initialises an PCBSFTPOperator object.

        :param local_filepath: the local filepath to grab the file from / save the file to.
        :param remote_filepath: the remote filepath where you want to put the file or pull the file from.
        :param create_intermediate_dirs: whether intermediate directories should be created on the
            remote host if they are missing. Defaults to False. Only applies to PUT operator.
        :param lookup_key: the key to look up in the config file to get all the information
            required to make the connection
        :param config_path: File path for the config file which contains the details to connect to the
            remote host
        """
        super().__init__(**kwargs)
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.create_intermediate_dirs = create_intermediate_dirs
        self.lookup_key = lookup_key
        self.config_path = config_path

    def _get_sftp_server_config(self) -> SftpServerConfig:
        """
        This private getter function reads the SFTP server info from the configuration and
        returns it in the SftpServerConfig format.

        :return: the configuration of the remote SFTP server.
        """
        if self.config_path is None:
            self.config_path = f"{DAGS_FOLDER}/config/sftp_dags_config.yaml"
        if self.lookup_key:
            lookup_key = self.lookup_key
        else:
            lookup_key = read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME]
        sftp_server_config = read_yamlfile_env(self.config_path)
        if isinstance(lookup_key, str):
            sftp_server_config = sftp_server_config.get(lookup_key)
        else:
            for key in lookup_key:
                if key not in sftp_server_config:
                    raise AirflowException(f"The lookup key {key} is not found. Valid options "
                                           f"are {list(sftp_server_config.keys())}")
                sftp_server_config = sftp_server_config.get(key)
        return SftpServerConfig(**sftp_server_config)

    def _get_ssh_hook(self) -> SSHHook:
        """
        This private helper function gets the private key and passphrase from the Vault server
        and returns an SSH hook to connect to the remote container

        :return: The SSH hook for the SFTP server.
        """
        sftp_server_config = self._get_sftp_server_config()
        vault_util = VaultUtilBuilder.build()
        private_key = vault_util.get_secret(sftp_server_config.secret_path, 'private_key')
        passphrase = vault_util.get_secret(sftp_server_config.secret_path, 'passphrase')
        ssh_hook = SSHHook(remote_host=sftp_server_config.hostname, username=sftp_server_config.username)
        ssh_hook.pkey = RSAKey.from_private_key(StringIO(private_key), password=passphrase)
        return ssh_hook

    def execute(self, context) -> Any:
        raise NotImplementedError()
