import logging
from airflow.exceptions import AirflowFailException
import requests
import google.auth.transport.requests
import google.oauth2.id_token
from google.auth.exceptions import GoogleAuthError
from requests.exceptions import Timeout, RequestException

logger = logging.getLogger(__name__)


def cloud_function_invoker(input_config: dict):
    """
        This method is for Invoking Cloud Functions from Airflow.

        :param input_config: Json Config which contains cloud function information
                           [location, project_id, function_name, data, headers, timeout]

        Example input_config:
        {
            "location": "us-central1",
            "project_id": "my-project",
            "function_name": "my-function",
            "data": {"key": "value"},
            "headers": {"Custom-Header": "value"}, # Optional
            "timeout": 300  # Optional, defaults to 300 seconds
        }
    """
    logger.info(f"input_config: {input_config}")

    required_fields = ["location", "project_id", "function_name", "data"]
    missing_fields = [field for field in required_fields if field not in input_config or input_config[field] is None]

    if missing_fields:
        raise AirflowFailException(f"Missing required fields: {missing_fields}")

    location = input_config["location"]
    project_id = input_config["project_id"]
    function_name = input_config["function_name"]
    data = input_config["data"]
    custom_headers = input_config.get("headers", {})
    timeout = input_config.get("timeout", 300)  # Default 5 minutes

    # Cloud Run service URL
    function_url = f"https://{location}-{project_id}.cloudfunctions.net/{function_name}"
    logger.info(f"Invoking Cloud Function:- {function_url}")

    try:
        # Fetch ID token for target audience (Function URL)
        auth_req = google.auth.transport.requests.Request()
        token_id = google.oauth2.id_token.fetch_id_token(auth_req, function_url)
        logger.info("Successfully obtained ID token for authentication")
    except GoogleAuthError as e:
        logger.error(f"Failed to obtain ID token: {e}")
        raise AirflowFailException(f"Authentication failed: {e}")

    # Construct headers with proper authorization
    headers = {
        "Authorization": f"Bearer {token_id}",
        "Content-Type": "application/json"
    }
    # Merge custom headers if provided (custom headers can override defaults except Authorization)
    if custom_headers:
        headers.update(custom_headers)
        # Ensure Authorization header is not overridden
        headers["Authorization"] = f"Bearer {token_id}"

    try:
        if data is not None:
            logger.info(f"Sending POST request with payload: {data}")
            resp = requests.post(function_url, headers=headers, json=data, timeout=timeout)
        else:
            logger.info("Sending GET request (no payload)")
            resp = requests.get(function_url, headers=headers, timeout=timeout)

        # Failure invoked
        if resp.status_code >= 400:
            logger.error(f"Cloud Function returned HTTP {resp.status_code}")
            raise AirflowFailException(f"Cloud Function returned HTTP {resp.status_code}, please review Cloud Function logs.")
        else:
            # Successfully invoked
            logger.info(f"Function response status: {resp.status_code}")
            logger.info(f"Function response headers: {dict(resp.headers)}")
            logger.info(f"Function response: {resp.text}")

        content_type = resp.headers.get("Content-Type", "").lower()
        if "application/json" in content_type:
            return resp.json()
        else:
            return resp.text

    except Timeout:
        logger.error(f"Request timed out after {timeout} seconds")
        raise AirflowFailException(f"Cloud Function request timed out after {timeout} seconds")

    except RequestException as network_err:
        logger.error(f"Network error occurred: {network_err}")
        raise AirflowFailException(f"Network error calling Cloud Function: {network_err}")

    except Exception as err:
        logger.error(f"Unexpected error occurred: {err}")
        raise AirflowFailException(f"Unexpected error calling Cloud Function: {err}")
