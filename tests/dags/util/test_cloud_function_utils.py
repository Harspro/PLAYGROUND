"""
Tests for the cloud_function_utils functions utilized throughout the data foundation repository.

"""

import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowFailException
from google.auth.exceptions import GoogleAuthError
from requests.exceptions import Timeout, RequestException

from util.cloud_function_utils import (
    cloud_function_invoker
)


def make_input_config(**kwargs):
    config = {
        "location": "us-central1",
        "project_id": "my-project",
        "function_name": "my-function",
        "data": {"key": "value"}
    }
    config.update(kwargs)
    return config


def test_missing_fields():
    # Remove required fields one by one
    for field in ["location", "project_id", "function_name", "data"]:
        config = make_input_config()
        config.pop(field)
        with pytest.raises(AirflowFailException, match="Missing required fields"):
            cloud_function_invoker(config)


def test_auth_error():
    with patch('google.oauth2.id_token.fetch_id_token', side_effect=GoogleAuthError("Auth error")):
        config = make_input_config()
        with pytest.raises(AirflowFailException, match="Authentication failed: Auth error"):
            cloud_function_invoker(config)


def test_successful_post_json_response():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json.return_value = {"result": "ok"}
    with patch('google.oauth2.id_token.fetch_id_token', return_value='dummy_token'):
        with patch('requests.post', return_value=mock_response) as mock_post:
            config = make_input_config()
            result = cloud_function_invoker(config)
            assert result == {"result": "ok"}
            mock_post.assert_called_once()


def test_successful_post_text_response():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "text/plain"}
    mock_response.text = "plain text"
    with patch('google.oauth2.id_token.fetch_id_token', return_value='dummy_token'):
        with patch('requests.post', return_value=mock_response):
            config = make_input_config()
            result = cloud_function_invoker(config)
            assert result == "plain text"


def test_post_http_error():
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_response.headers = {"Content-Type": "application/json"}
    with patch('google.oauth2.id_token.fetch_id_token', return_value='dummy_token'):
        with patch('requests.post', return_value=mock_response):
            config = make_input_config()
            with pytest.raises(AirflowFailException, match="Cloud Function returned HTTP 404"):
                cloud_function_invoker(config)


def test_timeout_error():
    with patch('google.oauth2.id_token.fetch_id_token', return_value='dummy_token'):
        with patch('requests.post', side_effect=Timeout):
            config = make_input_config()
            with pytest.raises(AirflowFailException, match="Cloud Function request timed out after 300 seconds"):
                cloud_function_invoker(config)


def test_network_error():
    with patch('google.oauth2.id_token.fetch_id_token', return_value='dummy_token'):
        with patch('requests.post', side_effect=RequestException("Network down")):
            config = make_input_config()
            with pytest.raises(AirflowFailException, match="Network error calling Cloud Function: Network down"):
                cloud_function_invoker(config)


def test_unexpected_error():
    with patch('google.oauth2.id_token.fetch_id_token', return_value='dummy_token'):
        with patch('requests.post', side_effect=Exception("Something weird")):
            config = make_input_config()
            with pytest.raises(AirflowFailException, match="Unexpected error calling Cloud Function: Something weird"):
                cloud_function_invoker(config)


def test_custom_headers():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json.return_value = {"result": "ok"}
    with patch('google.oauth2.id_token.fetch_id_token', return_value='dummy_token'):
        with patch('requests.post', return_value=mock_response) as mock_post:
            config = make_input_config(headers={"X-Test": "yes"})
            cloud_function_invoker(config)
            called_headers = mock_post.call_args[1]['headers']
            assert called_headers["X-Test"] == "yes"
            assert called_headers["Authorization"] == "Bearer dummy_token"
