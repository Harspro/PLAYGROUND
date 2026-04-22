"""
This module defines the `VaultUtil` class and the builder to communicate with it.
"""

import os
from dataclasses import dataclass
from typing import Optional

import requests

try:
    from airflow import settings as airflow_settings
    from airflow.exceptions import AirflowException
except Exception:
    airflow_settings = None

    class AirflowException(Exception):
        pass

from util.constants import GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME, VAULT_ROLE, VAULT_DEFAULT_ROLE


@dataclass
class VaultUtil:
    """
    VaultUtil is the utility class that handles the communication with the Vault (in all different
    deployment environments) to retrieve secrets from it.

    >>> vault_util = VaultUtilBuilder.build()
    >>> vault_util.get_secret("/path/to/secret", "key.of.secret", "vault_name:<optional>")
    'passw0rd!'
    """

    # The URL for the API for accessing meta-data for GCP.
    gcp_meta_url: str

    # The Base URL for the Vault server.
    base_url: str

    # The login path for the Vault app.
    login_path: str

    def get_secret(self, secret_path: str, secret_key: str, vault_name=None) -> str:
        """
        This method retrieves the secret with key `secret_key` located at the secret_path.

        :param secret_path: The path to secret in Vault.
        :param secret_key: The key of the secret.
        :return: The secret with key `secret_key` located at the secret_path.
        """
        # Retrieve the JWT token for the GCP service account.
        jwt_token = self._get_service_account_jwt_token(vault_name)
        # Use the JWT token to get the client token for the Vault.
        client_token = self._get_vault_client_token(jwt_token, vault_name)
        # Send the GET request to Vault to get the passwords.
        response = requests.get(url=self.base_url + secret_path,
                                headers={"Content-Type": "application/json", "X-Vault-Token": client_token},
                                verify=False)
        # Handle the HTTP Error.
        if not response.ok:
            raise AirflowException(response.text)
        # Return the secret if exists, otherwise, throw the AirflowException.
        secrets = response.json().get("data", {"data": None}).get("data", dict())
        if secret_key not in secrets:
            raise AirflowException("The key for the secret is invalid.")
        else:
            return secrets[secret_key]

    def _get_service_account_jwt_token(self, vault_name) -> str:
        """
        This private helper function retrieves the JWT token for audience vault/gcp-read-only
        through the GCP metadata API.

        :return: The JWT token for audience vault/gcp-read-only.
        """
        vault_role = VAULT_ROLE.get(vault_name, VAULT_DEFAULT_ROLE)
        headers = {"Metadata-Flavor": "Google"}
        params = {"audience": f"vault/{vault_role}", "format": "full"}
        response = requests.get(self.gcp_meta_url, headers=headers, params=params, verify=False)
        if not response.ok:
            raise AirflowException(response.text)
        return response.text

    def _get_vault_client_token(self, jwt_token: str, vault_name) -> str:
        """
        This private token retrieves the vault client token from the Vault server through the login API
        from the vault server.

        :param jwt_token: The JWT token for Vault login authentication.
        :return: The client token for vault.
        """
        vault_role = VAULT_ROLE.get(vault_name, VAULT_DEFAULT_ROLE)
        vault_token_payload = {"role": f"{vault_role}", "jwt": jwt_token}
        headers = {"Content-Type": "application/json", "Accepts": "application/json"}
        path = self.base_url + self.login_path
        response = requests.post(path, headers=headers, json=vault_token_payload, verify=False)
        if not response.ok:
            raise AirflowException(response.text)
        return response.json()["auth"]["client_token"]


class VaultUtilBuilder:
    """
    VaultUtilBuilder is the builder class for class VaultUtil.
    """

    @staticmethod
    def build() -> Optional[VaultUtil]:
        """
        This function builds the VaultConfig for the respective deployment environments,
        to access secrets stored at `secret_path` in the vault.

        If the VaultConfigBuilder is not called in the GCP environment, None will be returned.

        :return: The VaultConfig object being build.
        """
        if "GCP_PROJECT" not in os.environ:
            return None
        from util.miscutils import read_variable_or_file, read_yamlfile_env
        env_name = read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME]
        dags_folder = (
            airflow_settings.DAGS_FOLDER
            if airflow_settings is not None
            else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        )
        return VaultUtil(**read_yamlfile_env(f'{dags_folder}/config/vault_config.yaml')[env_name])
