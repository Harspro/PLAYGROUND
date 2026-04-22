"""
This file defines the data class SftpServerConfig.
"""
from dataclasses import dataclass


@dataclass
class SftpServerConfig:
    """
    This file defines the data structure for the configuration of an SFTP server.
    """

    # The hostname of the SFTP server.
    hostname: str

    # The username to sign in to the SFTP server.
    username: str

    # The path to the private key and paraphrase for this SFTP server in Vault.
    secret_path: str
