import os
import sys
import json
import base64
import requests

from google.api_core import exceptions as google_exceptions

# Add the project root directory to the Python path to resolve the import error
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# The function is decorated with @functions_framework.http, which means
# it expects a Flask Request object. We can mock this.
# Flask is a dependency of functions-framework.
from flask import Request

from main import scrape_and_upload


def _create_mock_pubsub_request(mocker, url: str) -> Request:
    """Helper to create a mock Flask request simulating a Pub/Sub push."""
    message_data = {"url": url}
    encoded_data = base64.b64encode(json.dumps(message_data).encode("utf-8"))
    pubsub_message = {
        "message": {
            "data": encoded_data.decode("utf-8"),
            "message_id": "test-message-id",
        },
        "subscription": "test-subscription",
    }
    mock_request = mocker.Mock(spec=Request)
    mock_request.get_json.return_value = pubsub_message
    return mock_request


def test_scrape_and_upload_success(mocker, monkeypatch):
    """
    Tests the happy path for the scrape_and_upload function,
    triggered by a Pub/Sub push notification.
    - Mocks a Pub/Sub push request containing the target URL.
    - Mocks successful web request.
    - Mocks GCS upload.
    - Verifies the function returns a success message and status code.
    """
    # 1. Setup: Mock environment variables and external dependencies
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    # The target URL is now passed via the request body, not an env var.
    target_url = "http://books.toscrape.com/index.html"

    # Mock the requests.get call
    mock_response = mocker.Mock()
    mock_response.text = (
        "<html><head><title>Test Page</title></head><body><h1>Hello</h1></body></html>"
    )
    mock_response.raise_for_status.return_value = None
    mock_requests_get = mocker.patch("main.requests.get", return_value=mock_response)

    # Mock the GCS client and its methods
    mock_storage_client = mocker.patch("main.storage_client")
    mock_bucket = mock_storage_client.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    # Mock a Flask request object simulating a Pub/Sub push notification.
    mock_flask_request = _create_mock_pubsub_request(mocker, target_url)
    # 2. Execution: Call the function
    response_text, status_code = scrape_and_upload(mock_flask_request)

    # 3. Assertions
    # Verify the web request was made correctly
    mock_requests_get.assert_called_once_with(
        target_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        },
        timeout=10,
    )

    # Verify the GCS upload was performed correctly
    expected_filename = "books.toscrape.com/index.html"

    mock_storage_client.bucket.assert_called_once_with("test-raw-bucket")
    mock_bucket.blob.assert_called_once_with(expected_filename)
    mock_blob.upload_from_string.assert_called_once_with(
        mock_response.text, content_type="text/html"
    )

    # Verify the function's return value
    assert status_code == 200
    assert "Successfully scraped" in response_text
    assert target_url in response_text
    assert f"gs://test-raw-bucket/{expected_filename}" in response_text


def test_scrape_and_upload_request_failure(mocker, monkeypatch):
    """
    Tests how scrape_and_upload handles a web request failure when triggered by Pub/Sub.
    - Mocks a failing web request (e.g., 404 Not Found).
    - Verifies that no file is uploaded to GCS.
    - Verifies the function returns an error message and status code 500.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://test.com/not-found.html"

    # Mock the requests.get call to simulate an HTTP error
    mock_response = mocker.Mock()
    http_error = requests.exceptions.HTTPError("404 Client Error: Not Found for url")
    mock_response.raise_for_status.side_effect = http_error
    mock_requests_get = mocker.patch("main.requests.get", return_value=mock_response)

    mock_storage_client = mocker.patch("main.storage_client")
    mock_flask_request = _create_mock_pubsub_request(mocker, target_url)

    # 2. Execution
    response_text, status_code = scrape_and_upload(mock_flask_request)

    # 3. Assertions
    mock_requests_get.assert_called_once()
    mock_storage_client.bucket.assert_not_called()
    assert status_code == 500
    assert "Error fetching URL" in response_text
    assert "404 Client Error" in response_text


def test_scrape_and_upload_timeout(mocker, monkeypatch):
    """
    Tests how scrape_and_upload handles a web request timeout when triggered by Pub/Sub.
    - Mocks a timeout from requests.get.
    - Verifies that no file is uploaded to GCS.
    - Verifies the function returns an error message and status code 500.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://test.com/slow-site.html"

    # Mock requests.get to raise a Timeout exception
    mock_requests_get = mocker.patch(
        "main.requests.get",
        side_effect=requests.exceptions.Timeout("Connection timed out"),
    )
    mock_storage_client = mocker.patch("main.storage_client")

    mock_flask_request = _create_mock_pubsub_request(mocker, target_url)

    # 2. Execution
    response_text, status_code = scrape_and_upload(mock_flask_request)

    # 3. Assertions
    mock_requests_get.assert_called_once()
    mock_storage_client.bucket.assert_not_called()
    assert status_code == 500
    assert "Error fetching URL" in response_text
    assert "Connection timed out" in response_text


def test_scrape_and_upload_connection_error(mocker, monkeypatch):
    """
    Tests how scrape_and_upload handles a generic network connection error when triggered by Pub/Sub.
    - Mocks a ConnectionError from requests.get.
    - Verifies that no file is uploaded to GCS.
    - Verifies the function returns an error message and status code 500.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://test.com/unreachable-site.html"

    # Mock requests.get to raise a ConnectionError
    mock_requests_get = mocker.patch(
        "main.requests.get",
        side_effect=requests.exceptions.ConnectionError(
            "Failed to establish a new connection"
        ),
    )
    mock_storage_client = mocker.patch("main.storage_client")

    mock_flask_request = _create_mock_pubsub_request(mocker, target_url)

    # 2. Execution
    response_text, status_code = scrape_and_upload(mock_flask_request)

    # 3. Assertions
    mock_requests_get.assert_called_once()
    mock_storage_client.bucket.assert_not_called()
    assert status_code == 500
    assert "Error fetching URL" in response_text
    assert "Failed to establish a new connection" in response_text


def test_scrape_and_upload_gcs_failure(mocker, monkeypatch):
    """
    Tests how scrape_and_upload handles a GCS upload failure when triggered by Pub/Sub.
    - Mocks a successful web request.
    - Mocks a failing GCS upload.
    - Verifies the function returns an error message and status code 500.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://test.com/index.html"

    # Mock the successful requests.get call
    mock_response = mocker.Mock()
    mock_response.text = "<html><body><h1>Success</h1></body></html>"
    mock_response.raise_for_status.return_value = None
    mock_requests_get = mocker.patch("main.requests.get", return_value=mock_response)

    # Mock the GCS client to raise an error on upload
    mock_storage_client = mocker.patch("main.storage_client")
    mock_bucket = mock_storage_client.bucket.return_value
    mock_blob = mock_bucket.blob.return_value
    gcs_error = google_exceptions.GoogleAPICallError("GCS upload failed for test")
    mock_blob.upload_from_string.side_effect = gcs_error

    mock_flask_request = _create_mock_pubsub_request(mocker, target_url)

    # 2. Execution
    response_text, status_code = scrape_and_upload(mock_flask_request)

    # 3. Assertions
    mock_requests_get.assert_called_once()
    mock_storage_client.bucket.assert_called_once_with("test-raw-bucket")
    mock_blob.upload_from_string.assert_called_once()
    assert status_code == 500
    assert "Error during GCS upload" in response_text
    assert "GCS upload failed for test" in response_text


def test_scrape_and_upload_uses_url_for_filename(mocker, monkeypatch):
    """
    Tests that the uploaded filename is based on the URL path for versioning
    when triggered by Pub/Sub.
    - Mocks successful web request.
    - Mocks GCS upload.
    - Verifies the blob name is derived directly from the URL path.
    """
    # 1. Setup: Mock environment variables and external dependencies
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    # The target URL is now passed via the request body.
    target_url = "http://example.com/some/path/page.html"

    # Mock the requests.get call
    mock_response = mocker.Mock()
    mock_response.text = (
        "<html><head><title>Test Page</title></head><body><h1>Hello</h1></body></html>"
    )
    mock_response.raise_for_status.return_value = None
    mock_requests_get = mocker.patch("main.requests.get", return_value=mock_response)

    # Mock the GCS client and its methods
    mock_storage_client = mocker.patch("main.storage_client")
    mock_bucket = mock_storage_client.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    mock_flask_request = _create_mock_pubsub_request(mocker, target_url)

    # 2. Execution: Call the function
    response_text, status_code = scrape_and_upload(mock_flask_request)

    # 3. Assertions
    # Verify the web request was made correctly
    mock_requests_get.assert_called_once_with(
        target_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        },
        timeout=10,
    )

    # Verify the GCS upload was performed correctly
    # The filename should be derived from the URL path, without randomness.
    expected_filename = "example.com/some/path/page.html"

    mock_storage_client.bucket.assert_called_once_with("test-raw-bucket")
    mock_bucket.blob.assert_called_once_with(expected_filename)
    mock_blob.upload_from_string.assert_called_once_with(
        mock_response.text, content_type="text/html"
    )

    # Verify the function's return value
    assert status_code == 200
    assert "Successfully scraped" in response_text
    assert target_url in response_text
    assert f"gs://test-raw-bucket/{expected_filename}" in response_text


def test_scrape_and_upload_root_url_creates_index(mocker, monkeypatch):
    """
    Tests that a root URL from a Pub/Sub message is correctly saved as
    'example.com/index.html'.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://example.com"  # Root URL with no path

    # Mock the requests.get call
    mock_response = mocker.Mock()
    mock_response.text = "<html><body>Root Page</body></html>"
    mock_response.raise_for_status.return_value = None
    mocker.patch("main.requests.get", return_value=mock_response)

    # Mock the GCS client
    mock_storage_client = mocker.patch("main.storage_client")
    mock_bucket = mock_storage_client.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    mock_flask_request = _create_mock_pubsub_request(mocker, target_url)

    # 2. Execution
    scrape_and_upload(mock_flask_request)

    # 3. Assertions
    # The filename should be the domain plus '/index.html'.
    expected_filename = "example.com/index.html"
    mock_bucket.blob.assert_called_once_with(expected_filename)
    mock_blob.upload_from_string.assert_called_once_with(
        mock_response.text, content_type="text/html"
    )
