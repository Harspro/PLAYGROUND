"""
This file defines the PCBSFTPPutOperator class.
"""

import logging
import os

from paramiko.sftp_client import SFTPClient

from pcf_operators.sftp_operator.sftp_operator import PCBSFTPOperator


class PCBSFTPPutOperator(PCBSFTPOperator):
    """
    Custom SFTP PUT Operator for transferring files from local host to a remote host.
    This operator uses SSHHook to open up a SFTP transport channel which allows for
    the file transfer to occur
    """

    def execute(self, context) -> None:
        def _make_intermediate_dirs(client: SFTPClient, remote_dir: str) -> None:
            if remote_dir == '':
                return
            elif remote_dir == '/':
                client.chdir('/')
            else:
                try:
                    client.chdir(remote_dir)
                except OSError:
                    dirname, basename = os.path.split(remote_dir.rstrip('/'))
                    _make_intermediate_dirs(client, dirname)
                    client.mkdir(basename)
                    client.chdir(basename)

        try:
            ssh_hook = self._get_ssh_hook()
            with ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                remote_folder = os.path.dirname(self.remote_filepath)
                if self.create_intermediate_dirs:
                    cwd = sftp_client.getcwd()
                    _make_intermediate_dirs(sftp_client, remote_folder)
                    sftp_client.chdir(cwd)
                logging.info(f'Starting to transfer file from {self.local_filepath} to {self.remote_filepath}')
                sftp_client.put(self.local_filepath, self.remote_filepath, confirm=True)
                logging.info('The file has been successfully moved!')
        except Exception as err:
            logging.error(f'Error while transferring file: {err}', exc_info=True, stack_info=True)
            raise err
