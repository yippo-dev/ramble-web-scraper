import os
import sys
import requests

from google.api_core import exceptions as google_exceptions

# Add the project root directory to the Python path to resolve the import error
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# The function is decorated with @functions_framework.http, which means
# it expects a Flask Request object. We can mock this.
# Flask is a dependency of functions-framework.
from flask import Request

from main import scrape_and_upload


def test_scrape_and_upload_success(mocker, monkeypatch):
    """
    Tests the happy path for the scrape_and_upload function.
    - Mocks successful web request.
    - Mocks GCS upload.
    - Verifies the function returns a success message and status code.
    """
    # 1. Setup: Mock environment variables and external dependencies
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    # Use a URL with a path component to test filename generation
    target_url = "http://test.com/index.html"
    monkeypatch.setenv("TARGET_URL", target_url)

    # Mock the requests.get call
    mock_response = mocker.Mock()
    mock_response.text = (
        "<html><head><title>Test Page</title></head><body><h1>Hello</h1></body></html>"
    )
    mock_response.raise_for_status.return_value = None
    mock_requests_get = mocker.patch("main.requests.get", return_value=mock_response)

    # Mock os.urandom for a predictable filename suffix
    mocker.patch("main.os.urandom", return_value=b"\xde\xad\xbe\xef\xca\xfe\xba\xbe")

    # Mock the GCS client and its methods
    mock_storage_client = mocker.patch("main.storage_client")
    mock_bucket = mock_storage_client.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    # The function expects a Flask request object, but doesn't use it.
    # A simple mock is sufficient.
    mock_flask_request = mocker.Mock(spec=Request)

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
    expected_random_suffix = "deadbeefcafebabe"
    expected_base_filename = "index_html"
    expected_filename = f"{expected_base_filename}_{expected_random_suffix}.html"

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
    Tests how scrape_and_upload handles a web request failure.
    - Mocks a failing web request (e.g., 404 Not Found).
    - Verifies that no file is uploaded to GCS.
    - Verifies the function returns an error message and status code 500.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://test.com/not-found.html"
    monkeypatch.setenv("TARGET_URL", target_url)

    # Mock the requests.get call to simulate an HTTP error
    mock_response = mocker.Mock()
    http_error = requests.exceptions.HTTPError("404 Client Error: Not Found for url")
    mock_response.raise_for_status.side_effect = http_error
    mock_requests_get = mocker.patch("main.requests.get", return_value=mock_response)

    mock_storage_client = mocker.patch("main.storage_client")
    mock_flask_request = mocker.Mock(spec=Request)

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
    Tests how scrape_and_upload handles a web request timeout.
    - Mocks a timeout from requests.get.
    - Verifies that no file is uploaded to GCS.
    - Verifies the function returns an error message and status code 500.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://test.com/slow-site.html"
    monkeypatch.setenv("TARGET_URL", target_url)

    # Mock requests.get to raise a Timeout exception
    mock_requests_get = mocker.patch(
        "main.requests.get",
        side_effect=requests.exceptions.Timeout("Connection timed out"),
    )

    mock_storage_client = mocker.patch("main.storage_client")
    mock_flask_request = mocker.Mock(spec=Request)

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
    Tests how scrape_and_upload handles a generic network connection error.
    - Mocks a ConnectionError from requests.get.
    - Verifies that no file is uploaded to GCS.
    - Verifies the function returns an error message and status code 500.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://test.com/unreachable-site.html"
    monkeypatch.setenv("TARGET_URL", target_url)

    # Mock requests.get to raise a ConnectionError
    mock_requests_get = mocker.patch(
        "main.requests.get",
        side_effect=requests.exceptions.ConnectionError(
            "Failed to establish a new connection"
        ),
    )

    mock_storage_client = mocker.patch("main.storage_client")
    mock_flask_request = mocker.Mock(spec=Request)

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
    Tests how scrape_and_upload handles a GCS upload failure.
    - Mocks a successful web request.
    - Mocks a failing GCS upload.
    - Verifies the function returns an error message and status code 500.
    """
    # 1. Setup
    monkeypatch.setenv("RAW_DATA_BUCKET", "test-raw-bucket")
    target_url = "http://test.com/index.html"
    monkeypatch.setenv("TARGET_URL", target_url)

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

    mock_flask_request = mocker.Mock(spec=Request)

    # 2. Execution
    response_text, status_code = scrape_and_upload(mock_flask_request)

    # 3. Assertions
    mock_requests_get.assert_called_once()
    mock_storage_client.bucket.assert_called_once_with("test-raw-bucket")
    mock_blob.upload_from_string.assert_called_once()
    assert status_code == 500
    assert "Error during GCS upload" in response_text
    assert "GCS upload failed for test" in response_text
