"""
This file defines the PCBSFTPGetOperator class.
"""

import logging

from pcf_operators.sftp_operator.sftp_operator import PCBSFTPOperator


class PCBSFTPGetOperator(PCBSFTPOperator):
    """
    Custom SFTP GET Operator for transferring files from the remote host to the local host.
    This operator uses SSHHook to open up a SFTP transport channel which allows for the file
    transfer to occur.
    """

    def execute(self, context) -> None:
        try:
            ssh_hook = self._get_ssh_hook()
            with ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                logging.info(f'Starting to transfer file from {self.remote_filepath} to {self.local_filepath}')
                sftp_client.get(remotepath=self.remote_filepath, localpath=self.local_filepath)
                logging.info('The file has been successfully moved!')
        except Exception as err:
            logging.error(f'Error while transferring file: {err}', exc_info=True, stack_info=True)
            raise err
