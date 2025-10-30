import os
import sys
import json

from google.api_core import exceptions as google_exceptions

# Add the project root directory to the Python path to resolve the import error
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from main import process_data


def test_process_data_fallback_extracts_all_links(mocker, monkeypatch):
    """
    Tests that if a domain is not in config.json, the function falls back
    to extracting all `<a>` links from the page, not headings.
    - Mocks a GCS CloudEvent.
    - Mocks a non-existent config file to trigger the fallback.
    - Mocks GCS download and upload.
    - Verifies that all links are extracted and uploaded.
    """
    # 1. Setup
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")

    # Mock the CloudEvent for a domain that won't be in the config
    source_bucket = "test-raw-bucket"
    source_file = "unconfigured-domain.com/page.html"
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": source_bucket, "name": source_file}

    # Mock the config file to be not found, which triggers the fallback logic.
    mocker.patch("builtins.open", side_effect=FileNotFoundError)

    # Mock GCS client and its interactions
    mock_storage_client = mocker.patch("main.storage_client")

    # Mock the download part
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.return_value = """
    <html><body>
        <h1>Some Page</h1>
        <p>A link to <a href="/about.html">another page</a>.</p>
        <div><a href="https://external.com/resource">External Link</a></div>
        <a href="products/product1.html">Product 1</a>
    </body></html>
    """
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

    process_data(mock_cloud_event)

    expected_json_content = {
        "source_file": source_file,
        "next_page_url": None,
        "result_urls": [
            "http://unconfigured-domain.com/about.html",
            "https://external.com/resource",
            "http://unconfigured-domain.com/products/product1.html",
        ],
    }
    expected_json_string = json.dumps(expected_json_content, indent=2)
    expected_processed_filename = "unconfigured-domain.com/page.json"

    mock_processed_bucket.blob.assert_called_once_with(expected_processed_filename)
    mock_processed_blob.upload_from_string.assert_called_once_with(
        expected_json_string, content_type="application/json"
    )


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
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": source_bucket, "name": source_file}

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
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": source_bucket, "name": source_file}

    mock_storage_client = mocker.patch("main.storage_client")

    # Mock the download part (successful)
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.return_value = (
        "<html><body><h1>Title 1</h1></body></html>"
    )
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
    # Mock the config file to be not found, which triggers the fallback logic.
    mocker.patch("builtins.open", side_effect=FileNotFoundError)

    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")

    source_bucket = "test-raw-bucket"
    source_file = "empty_file.html"
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": source_bucket, "name": source_file}

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
    expected_json_content = {
        "source_file": source_file,
        "next_page_url": None,
        "result_urls": [],
    }
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
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": source_bucket, "name": source_file}

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


def test_process_data_uses_bundled_config_for_link_extraction(mocker, monkeypatch):
    """
    Tests that process_data can extract links based on a single, bundled
    configuration file (`config.json`).
    - Mocks a GCS CloudEvent for an HTML file.
    - Mocks download of the HTML file.
    - Mocks the reading of a local `config.json` file.
    - Verifies that the correct domain's config is used for extraction.
    - Verifies that the extracted links (resolved to absolute URLs) are
      uploaded as a JSON object to the processed bucket.
    """
    # 1. Setup
    # Mock environment variables
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")

    # Mock the CloudEvent for the source HTML file
    source_bucket = "test-raw-bucket"
    source_file = "books.toscrape.com/index.html"
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": source_bucket, "name": source_file}

    # Mock HTML content
    html_content = """
    <!DOCTYPE html>
    <html>
    <body>
        <article class="product_pod">
            <h3><a href="catalogue/a-light-in-the-attic_1000/index.html" title="A Light in the Attic">A Light in the Attic</a></h3>
        </article>
        <article class="product_pod">
            <h3><a href="catalogue/tipping-the-velvet_999/index.html" title="Tipping the Velvet">Tipping the Velvet</a></h3>
        </article>
        <ul class="pager">
            <li class="next"><a href="catalogue/page-2.html">next</a></li>
        </ul>
    </body>
    </html>
    """

    # Mock the content of the bundled config.json file.
    # The function should be able to look up the config by domain.
    config_data = {
        "books.toscrape.com": {
            "next_page_selector": ".pager .next a",
            "result_link_selector": "article.product_pod h3 a",
        },
        "another.domain.com": {
            "next_page_selector": ".next-link",
            "result_link_selector": ".result-item",
        },
    }
    # Mock the file system read for 'config.json'. The implementation will
    # need to open this file, and json.load() will use the read_data.
    mocker.patch("builtins.open", mocker.mock_open(read_data=json.dumps(config_data)))

    # Mock GCS client and its interactions
    mock_storage_client = mocker.patch("main.storage_client")

    # Mocks for the raw and processed buckets
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.return_value = html_content
    mock_raw_bucket.blob.return_value = mock_raw_blob

    mock_processed_bucket = mocker.Mock()
    mock_processed_blob = mocker.Mock()
    mock_processed_bucket.blob.return_value = mock_processed_blob

    # Side effect to return the correct mock bucket based on name
    def bucket_side_effect(bucket_name):
        if bucket_name == source_bucket:
            return mock_raw_bucket
        if bucket_name == "test-processed-bucket":
            return mock_processed_bucket
        return mocker.Mock()

    mock_storage_client.bucket.side_effect = bucket_side_effect

    # 2. Execution
    process_data(mock_cloud_event)

    # 3. Assertions
    # Verify correct bucket and blob were accessed for the source HTML
    mock_raw_bucket.blob.assert_called_once_with(source_file)

    # Verify the output was uploaded to the correct place
    expected_processed_filename = "books.toscrape.com/index.json"
    mock_processed_bucket.blob.assert_called_once_with(expected_processed_filename)

    # Verify the content of the uploaded JSON
    expected_json_content = {
        "source_file": source_file,
        "next_page_url": "http://books.toscrape.com/catalogue/page-2.html",
        "result_urls": [
            "http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html",
            "http://books.toscrape.com/catalogue/tipping-the-velvet_999/index.html",
        ],
    }
    mock_processed_blob.upload_from_string.assert_called_once()
    uploaded_string = mock_processed_blob.upload_from_string.call_args[0][0]
    uploaded_data = json.loads(uploaded_string)
    content_type_arg = mock_processed_blob.upload_from_string.call_args[1][
        "content_type"
    ]

    assert uploaded_data == expected_json_content
    assert content_type_arg == "application/json"


def test_process_data_publishes_next_page_url(mocker, monkeypatch):
    """
    Tests that if a 'next_page_url' is found, it is published to the
    Pub/Sub topic for the next crawl job.
    - Mocks a GCS CloudEvent.
    - Mocks config.json to find a next page link.
    - Mocks GCS download and upload.
    - Mocks the Pub/Sub publisher client.
    - Verifies that the publisher's `publish` method is called with the
      correct topic and the extracted next page URL.
    """
    # 1. Setup
    # Mock environment variables
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")
    topic_name = "projects/ramble-web-scraper/topics/crawl-queue"
    monkeypatch.setenv("CRAWL_QUEUE_TOPIC", topic_name)

    # Mock the CloudEvent
    source_bucket = "test-raw-bucket"
    source_file = "books.toscrape.com/page-1.html"
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": source_bucket, "name": source_file}

    # Mock HTML with a next page link
    next_page_relative_url = "catalogue/page-2.html"
    next_page_absolute_url = f"http://books.toscrape.com/{next_page_relative_url}"
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <body>
        <ul class="pager">
            <li class="next"><a href="{next_page_relative_url}">next</a></li>
        </ul>
    </body>
    </html>
    """

    # Mock config.json
    config_data = {
        "books.toscrape.com": {
            "next_page_selector": ".pager .next a",
            "result_link_selector": "article.product_pod h3 a",
        }
    }
    mocker.patch("builtins.open", mocker.mock_open(read_data=json.dumps(config_data)))

    # Mock GCS client
    mock_storage_client = mocker.patch("main.storage_client")
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.return_value = html_content
    mock_raw_bucket.blob.return_value = mock_raw_blob
    mock_processed_bucket = mocker.Mock()
    mock_processed_blob = mocker.Mock()
    mock_processed_bucket.blob.return_value = mock_processed_blob

    def bucket_side_effect(bucket_name):
        if bucket_name == source_bucket:
            return mock_raw_bucket
        if bucket_name == "test-processed-bucket":
            return mock_processed_bucket
        return mocker.Mock()

    mock_storage_client.bucket.side_effect = bucket_side_effect

    # Mock Pub/Sub publisher client, which will be added to main.py
    mock_publisher = mocker.patch("main.pubsub_publisher")

    # 2. Execution
    process_data(mock_cloud_event)

    # 3. Assertions
    # Verify Pub/Sub publish was called correctly
    mock_publisher.publish.assert_called_once()

    # Check the arguments passed to publish
    call_args = mock_publisher.publish.call_args
    assert call_args.args[0] == topic_name

    # Decode the data payload for verification
    published_data_bytes = call_args.kwargs["data"]
    published_data = json.loads(published_data_bytes.decode("utf-8"))

    expected_payload = {"url": next_page_absolute_url}
    assert published_data == expected_payload


def test_process_data_does_not_publish_if_no_next_link(mocker, monkeypatch):
    """
    Tests that if no 'next_page_url' is found, the Pub/Sub publisher is
    not called.
    """
    # 1. Setup
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")
    monkeypatch.setenv("CRAWL_QUEUE_TOPIC", "some/topic")

    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": "test-raw-bucket", "name": "domain.com/last.html"}

    mocker.patch("builtins.open", side_effect=FileNotFoundError)
    mock_storage_client = mocker.patch("main.storage_client")
    mock_storage_client.bucket.return_value.blob.return_value.download_as_text.return_value = "<html></html>"
    mock_publisher = mocker.patch("main.pubsub_publisher")

    # 2. Execution
    process_data(mock_cloud_event)

    # 3. Assertions
    mock_publisher.publish.assert_not_called()


def test_process_data_handles_www_subdomain(mocker, monkeypatch):
    """
    Tests that if a domain has a 'www.' prefix, the function can still find
    the configuration for the base domain (e.g., 'recreation.gov').
    """
    # 1. Setup
    monkeypatch.setenv("PROCESSED_DATA_BUCKET", "test-processed-bucket")
    monkeypatch.setenv("CRAWL_QUEUE_TOPIC", "some/topic")

    # Mock the CloudEvent for a file from a 'www.' subdomain
    source_bucket = "test-raw-bucket"
    source_file = "www.recreation.gov/search.html"  # Note the 'www.'
    mock_cloud_event = mocker.Mock()
    mock_cloud_event.data = {"bucket": source_bucket, "name": source_file}

    # Mock HTML content from recreation.gov
    html_content = """
    <html><body>
        <div class="rec-flex-card-body-wrap">
            <a href="/camping/campgrounds/231875">
                <h2 class="rec-flex-card-title h5-normal">BUFFALO CAMPGROUND</h2>
            </a>
        </div>
        <a href="/some-other-link">An irrelevant link</a>
    </body></html>
    """

    # Mock the config.json file content
    config_data = {
        "recreation.gov": {
            "next_page_selector": None,
            "result_link_selector": "a:has(h2.rec-flex-card-title)",
        }
    }
    mocker.patch("builtins.open", mocker.mock_open(read_data=json.dumps(config_data)))

    # Mock GCS and Pub/Sub
    mock_storage_client = mocker.patch("main.storage_client")
    mock_raw_bucket = mocker.Mock()
    mock_raw_blob = mocker.Mock()
    mock_raw_blob.download_as_text.return_value = html_content
    mock_raw_bucket.blob.return_value = mock_raw_blob
    mock_processed_bucket = mocker.Mock()
    mock_processed_blob = mocker.Mock()
    mock_processed_bucket.blob.return_value = mock_processed_blob

    def bucket_side_effect(bucket_name):
        if bucket_name == source_bucket:
            return mock_raw_bucket
        if bucket_name == "test-processed-bucket":
            return mock_processed_bucket
        return mocker.Mock()

    mock_storage_client.bucket.side_effect = bucket_side_effect
    mock_publisher = mocker.patch("main.pubsub_publisher")

    # 2. Execution
    process_data(mock_cloud_event)

    # 3. Assertions
    # Verify the correct JSON was generated and uploaded
    expected_json_content = {
        "source_file": source_file,
        "next_page_url": None,
        "result_urls": ["http://www.recreation.gov/camping/campgrounds/231875"],
    }
    mock_processed_blob.upload_from_string.assert_called_once()
    uploaded_string = mock_processed_blob.upload_from_string.call_args[0][0]
    uploaded_data = json.loads(uploaded_string)

    # This assertion will fail with the current code because it will fall back
    # to extracting ALL links, not just the configured ones.
    assert uploaded_data == expected_json_content

    # Verify no "next page" was published
    mock_publisher.publish.assert_not_called()
