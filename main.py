import json
import os

import functions_framework
import requests
from bs4 import BeautifulSoup
from google.api_core import exceptions as google_exceptions
from google.cloud import storage

# --- Global Clients ---
# Initializing clients globally to reuse connections across function
# invocations, which is a performance best practice for Cloud Functions.
storage_client = storage.Client()

# A realistic User-Agent can help avoid being blocked by some websites.
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"


@functions_framework.http
def scrape_and_upload(request):
    """
    An HTTP-triggered Cloud Function that scrapes a website and uploads
    the raw HTML to a Google Cloud Storage bucket.
    """
    # Load configuration within the function call to allow for test mocking.
    target_url = os.environ.get("TARGET_URL", "http://books.toscrape.com/")
    raw_data_bucket = os.environ.get("RAW_DATA_BUCKET")

    # Ensure the destination bucket is configured.
    if not raw_data_bucket:
        log_message = (
            "Configuration error: RAW_DATA_BUCKET environment variable not set."
        )
        print(json.dumps({"message": log_message, "severity": "ERROR"}))
        return log_message, 500

    try:
        # 1. Scrape the target website.
        headers = {"User-Agent": USER_AGENT}
        # Use a timeout to prevent the function from hanging indefinitely.
        response = requests.get(target_url, headers=headers, timeout=10)
        # Raise an HTTPError for bad responses (4xx or 5xx).
        response.raise_for_status()
        html_content = response.text

        # 2. Generate a unique filename for the raw data.
        # Using os.urandom provides a secure way to get a random suffix.
        random_suffix = os.urandom(8).hex()
        # Sanitize the base URL to create a safe filename.
        base_filename = os.path.basename(target_url).split("?")[0].replace(".", "_")
        filename = f"{base_filename}_{random_suffix}.html"

        # 3. Upload the raw HTML to Cloud Storage.
        bucket = storage_client.bucket(raw_data_bucket)
        blob = bucket.blob(filename)
        blob.upload_from_string(html_content, content_type="text/html")

        success_message = (
            f"Successfully scraped {target_url} and uploaded to "
            f"gs://{raw_data_bucket}/{filename}"
        )
        print(
            json.dumps(
                {
                    "message": success_message,
                    "severity": "INFO",
                    "target_url": target_url,
                    "destination_blob": f"gs://{raw_data_bucket}/{filename}",
                }
            )
        )
        return success_message, 200

    except requests.exceptions.RequestException as e:
        # This catches connection errors, timeouts, and HTTP errors.
        log_payload = {
            "message": f"Error fetching URL: {target_url}",
            "severity": "ERROR",
            "error": str(e),
            "target_url": target_url,
        }
        print(json.dumps(log_payload))
        return f"Error fetching URL {target_url}: {e}", 500
    except Exception as e:
        # A general exception handler for other issues (e.g., GCS errors).
        log_payload = {
            "message": "An unexpected error occurred during scrape and upload.",
            "severity": "ERROR",
            "error": str(e),
            "target_url": target_url,
        }
        print(json.dumps(log_payload))
        return f"Error during GCS upload or other failure: {e}", 500


@functions_framework.cloud_event
def process_data(cloud_event):
    """
    A CloudEvent-triggered function that processes a raw HTML file from GCS,
    extracts all h1, h2, and h3 headings, and saves the result as a JSON
    file in another GCS bucket.
    """
    # Load configuration within the function call to allow for test mocking.
    processed_data_bucket = os.environ.get("PROCESSED_DATA_BUCKET")

    # Ensure the destination bucket is configured.
    if not processed_data_bucket:
        log_message = (
            "Configuration error: PROCESSED_DATA_BUCKET environment variable not set."
        )
        print(json.dumps({"message": log_message, "severity": "ERROR"}))
        return

    try:
        # 1. Parse event data to get the source bucket and file name.
        # This payload structure is typical for events from Cloud Audit Logs for GCS.
        # e.g., projects/gcp-project/buckets/my-raw-bucket/objects/some/file.html
        resource_name = cloud_event.data["protoPayload"]["resourceName"]
        parts = resource_name.split("/")
        bucket_name = parts[3]
        file_name = "/".join(parts[5:])

        print(
            json.dumps(
                {
                    "message": f"Processing file: {file_name}",
                    "severity": "INFO",
                    "source_bucket": bucket_name,
                    "source_file": file_name,
                }
            )
        )

        # 2. Download and parse the source HTML file.
        raw_bucket = storage_client.bucket(bucket_name)
        raw_blob = raw_bucket.blob(file_name)
        html_content = raw_blob.download_as_text()

        soup = BeautifulSoup(html_content, "html.parser")
        headings = [h.get_text(strip=True) for h in soup.find_all(["h1", "h2", "h3"])]

        # 3. Prepare and upload the processed data as JSON.
        processed_data = {"source_file": file_name, "headings": headings}
        json_data = json.dumps(processed_data, indent=2)

        # Create a corresponding .json filename.
        processed_file_name = f"{os.path.splitext(file_name)[0]}.json"
        processed_bucket = storage_client.bucket(processed_data_bucket)
        processed_blob = processed_bucket.blob(processed_file_name)
        processed_blob.upload_from_string(json_data, content_type="application/json")

        success_message = (
            f"Successfully processed {file_name} and uploaded to "
            f"gs://{processed_data_bucket}/{processed_file_name}"
        )
        print(
            json.dumps(
                {
                    "message": success_message,
                    "severity": "INFO",
                    "source_file": file_name,
                    "destination_bucket": processed_data_bucket,
                    "destination_file": processed_file_name,
                    "headings_found": len(headings),
                }
            )
        )

    except (
        google_exceptions.NotFound,
        google_exceptions.Forbidden,
        UnicodeDecodeError,
        KeyError,
    ) as e:
        # Handle common errors like file not found, permission issues,
        # non-text content, or malformed event data.
        # We try to get file_name and bucket_name if they exist for better context.
        file_name = locals().get("file_name", "unknown")
        bucket_name = locals().get("bucket_name", "unknown")
        log_payload = {
            "message": "Error processing file.",
            "severity": "ERROR",
            "error": str(e),
            "error_type": type(e).__name__,
            "source_file": file_name,
            "source_bucket": bucket_name,
        }
        print(json.dumps(log_payload))
    except Exception as e:
        # A general exception handler for any other unexpected errors.
        file_name = locals().get("file_name", "unknown")
        bucket_name = locals().get("bucket_name", "unknown")
        log_payload = {
            "message": "An unexpected error occurred during data processing.",
            "severity": "ERROR",
            "error": str(e),
            "error_type": type(e).__name__,
            "source_file": file_name,
            "source_bucket": bucket_name,
        }
        print(json.dumps(log_payload))
