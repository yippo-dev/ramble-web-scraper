import os
import sys
import json

from google.api_core import exceptions as google_exceptions

# Add the project root directory to the Python path to resolve the import error
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from main import process_data


def test_process_data_success(mocker, monkeypatch):
    """
    Tests the happy path for the process_data function.
    - Mocks a GCS CloudEvent.
    - Mocks GCS download and upload.
    - Verifies the data is processed correctly and uploaded to the right place.
    """
    # 1. Setup
    # Mock environment variables
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")

    # Mock the CloudEvent object. The function expects a specific payload
    # structure for GCS object creation events.
    source_bucket = "test-raw-bucket"
    source_file = "test_data_123.html"
    resource_name = f"projects/_/buckets/{source_bucket}/objects/{source_file}"
    mock_event_data = {"protoPayload": {"resourceName": resource_name}}
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = mock_event_data

    # Mock GCS client and its interactions
    mock_storage_client = mocker.patch("main.storage_client")

    # Mock the download part
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.return_value = "<html><body><h1>Title 1</h1><h2>Subtitle 2</h2><h3>Another Heading</h3></body></html>"
    mock_raw_bucket.blob.return_value = mock_raw_blob

    # Mock the upload part
    mock_processed_bucket = mocker.Mock()
    mock_processed_blob = mocker.Mock()
    mock_processed_bucket.blob.return_value = mock_processed_blob

    # Make the client's bucket() method return the correct mock bucket
    # depending on the argument.
    def bucket_side_effect(bucket_name):
        if bucket_name == source_bucket:
            return mock_raw_bucket
        elif bucket_name == "test-processed-bucket":
            return mock_processed_bucket
        return mocker.Mock()

    mock_storage_client.bucket.side_effect = bucket_side_effect

    # 2. Execution
    process_data(mock_cloud_event)

    # 3. Assertions
    mock_storage_client.bucket.assert_any_call(source_bucket)
    mock_raw_bucket.blob.assert_called_once_with(source_file)
    mock_raw_blob.download_as_text.assert_called_once()

    expected_json_content = {"source_file": source_file, "headings": ["Title 1", "Subtitle 2", "Another Heading"]}
    expected_json_string = json.dumps(expected_json_content, indent=2)
    expected_processed_filename = "test_data_123.json"

    mock_storage_client.bucket.assert_any_call("test-processed-bucket")
    mock_processed_bucket.blob.assert_called_once_with(expected_processed_filename)
    mock_processed_blob.upload_from_string.assert_called_once_with(expected_json_string, content_type="application/json")


def test_process_data_invalid_content(mocker, monkeypatch):
    """
    Tests how process_data handles a file with invalid content (e.g., binary).
    - Mocks a GCS CloudEvent.
    - Mocks GCS download to raise UnicodeDecodeError.
    - Verifies that no file is uploaded to the processed bucket.
    """
    # 1. Setup
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")

    source_bucket = "test-raw-bucket"
    source_file = "not_html.jpg"
    resource_name = f"projects/_/buckets/{source_bucket}/objects/{source_file}"
    mock_event_data = {"protoPayload": {"resourceName": resource_name}}
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = mock_event_data

    mock_storage_client = mocker.patch("main.storage_client")

    # Mock the download part to raise an error
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.side_effect = UnicodeDecodeError(
        "utf-8", b"\x89PNG\r\n\x1a\n", 0, 1, "invalid start byte"
    )
    mock_raw_bucket.blob.return_value = mock_raw_blob
    mock_storage_client.bucket.return_value = mock_raw_bucket

    # 2. Execution
    process_data(mock_cloud_event)

    # 3. Assertions
    mock_storage_client.bucket.assert_called_once_with(source_bucket)
    mock_raw_bucket.blob.assert_called_once_with(source_file)
    mock_raw_blob.download_as_text.assert_called_once()


def test_process_data_gcs_upload_failure(mocker, monkeypatch):
    """
    Tests how process_data handles a GCS upload failure.
    - Mocks a GCS CloudEvent.
    - Mocks a successful GCS download.
    - Mocks a failing GCS upload.
    - Verifies the function attempts the upload and handles the error.
    """
    # 1. Setup
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")

    source_bucket = "test-raw-bucket"
    source_file = "test_data_123.html"
    resource_name = f"projects/_/buckets/{source_bucket}/objects/{source_file}"
    mock_event_data = {"protoPayload": {"resourceName": resource_name}}
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = mock_event_data

    mock_storage_client = mocker.patch("main.storage_client")

    # Mock the download part (successful)
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.return_value = "<html><body><h1>Title 1</h1></body></html>"
    mock_raw_bucket.blob.return_value = mock_raw_blob

    # Mock the upload part (failure)
    mock_processed_bucket = mocker.Mock()
    mock_processed_blob = mocker.Mock()
    gcs_error = google_exceptions.GoogleAPICallError("GCS upload failed for test")
    mock_processed_blob.upload_from_string.side_effect = gcs_error
    mock_processed_bucket.blob.return_value = mock_processed_blob

    def bucket_side_effect(bucket_name):
        if bucket_name == source_bucket:
            return mock_raw_bucket
        elif bucket_name == "test-processed-bucket":
            return mock_processed_bucket
        return mocker.Mock()

    mock_storage_client.bucket.side_effect = bucket_side_effect

    # 2. Execution - The function should catch the exception and not crash.
    process_data(mock_cloud_event)

    # 3. Assertions
    # Verify download was called correctly
    mock_raw_blob.download_as_text.assert_called_once()
    # Verify upload was attempted
    mock_processed_blob.upload_from_string.assert_called_once()


def test_process_data_empty_file(mocker, monkeypatch):
    """
    Tests how process_data handles an empty file.
    - Mocks a GCS CloudEvent for an empty file.
    - Mocks GCS download to return an empty string.
    - Verifies that a JSON file with an empty headings list is uploaded.
    """
    # 1. Setup
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")

    source_bucket = "test-raw-bucket"
    source_file = "empty_file.html"
    resource_name = f"projects/_/buckets/{source_bucket}/objects/{source_file}"
    mock_event_data = {"protoPayload": {"resourceName": resource_name}}
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = mock_event_data

    mock_storage_client = mocker.patch("main.storage_client")

    # Mock the download part to return an empty string
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.return_value = ""
    mock_raw_bucket.blob.return_value = mock_raw_blob

    # Mock the upload part
    mock_processed_bucket = mocker.Mock()
    mock_processed_blob = mocker.Mock()
    mock_processed_bucket.blob.return_value = mock_processed_blob

    def bucket_side_effect(bucket_name):
        if bucket_name == source_bucket:
            return mock_raw_bucket
        elif bucket_name == "test-processed-bucket":
            return mock_processed_bucket
        return mocker.Mock()

    mock_storage_client.bucket.side_effect = bucket_side_effect

    # 2. Execution
    process_data(mock_cloud_event)

    # 3. Assertions
    expected_json_content = {"source_file": source_file, "headings": []}
    expected_json_string = json.dumps(expected_json_content, indent=2)
    expected_processed_filename = "empty_file.json"

    mock_processed_bucket.blob.assert_called_once_with(expected_processed_filename)
    mock_processed_blob.upload_from_string.assert_called_once_with(
        expected_json_string, content_type="application/json"
    )


def test_process_data_gcs_download_failure(mocker, monkeypatch):
    """
    Tests how process_data handles a GCS download failure (e.g., file not found).
    - Mocks a GCS CloudEvent.
    - Mocks GCS download to raise a NotFound error.
    - Verifies that no file is uploaded to the processed bucket.
    """
    # 1. Setup
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")

    source_bucket = "test-raw-bucket"
    source_file = "deleted_file.html"
    resource_name = f"projects/_/buckets/{source_bucket}/objects/{source_file}"
    mock_event_data = {"protoPayload": {"resourceName": resource_name}}
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = mock_event_data

    mock_storage_client = mocker.patch("main.storage_client")

    # Mock the download part to raise a NotFound error
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.side_effect = google_exceptions.NotFound(
        "File not found for testing"
    )
    mock_raw_bucket.blob.return_value = mock_raw_blob
    mock_storage_client.bucket.return_value = mock_raw_bucket

    # 2. Execution
    process_data(mock_cloud_event)

    # 3. Assertions
    # Verify the download was attempted, but no upload occurred.
    # The `assert_called_once_with` implicitly checks that the `bucket()`
    # method was not called a second time for the processed bucket.
    mock_storage_client.bucket.assert_called_once_with(source_bucket)
    mock_raw_bucket.blob.assert_called_once_with(source_file)
    mock_raw_blob.download_as_text.assert_called_once()
