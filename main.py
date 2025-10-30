import json
import base64
import os
from urllib.parse import urlparse, urljoin

import functions_framework
import requests
from bs4 import BeautifulSoup
from google.api_core import exceptions as google_exceptions
from google.cloud import pubsub_v1, storage

# --- Global Clients ---
# Initializing clients globally to reuse connections across function
# invocations, which is a performance best practice for Cloud Functions.
storage_client = storage.Client()
pubsub_publisher = pubsub_v1.PublisherClient()


def load_config():
    """Loads the scraper configuration from config.json."""
    try:
        with open("config.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(
            json.dumps(
                {
                    "message": "config.json not found. Using default processing.",
                    "severity": "WARNING",
                }
            )
        )
        return {}
    except json.JSONDecodeError as e:
        print(
            json.dumps(
                {"message": f"Error decoding config.json: {e}", "severity": "ERROR"}
            )
        )
        return {}


# A realistic User-Agent can help avoid being blocked by some websites.
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"


@functions_framework.http
def scrape_and_upload(request):
    """
    An HTTP-triggered Cloud Function that scrapes a website and uploads
    the raw HTML to a Google Cloud Storage bucket. The function is triggered
    by a Pub/Sub push subscription. The message body must contain a
    base64-encoded JSON payload with a "url" key.
    e.g. {"message": {"data": "eyJ1cmwiOiAiaHR0cDovL2V4YW1wbGUuY29tIn0="}}
    """
    # Initialize target_url for use in logging/error handlers
    target_url = None
    raw_data_bucket = os.environ.get("RAW_DATA_BUCKET")

    # Ensure the destination bucket is configured.
    if not raw_data_bucket:
        log_message = (
            "Configuration error: RAW_DATA_BUCKET environment variable not set."
        )
        print(json.dumps({"message": log_message, "severity": "ERROR"}))
        return log_message, 500

    try:
        # 1. Parse the Pub/Sub message to get the target URL.
        request_json = request.get_json(silent=True)
        if not request_json or "message" not in request_json:
            log_message = "Invalid Pub/Sub message format: missing 'message' key."
            print(json.dumps({"message": log_message, "severity": "ERROR"}))
            return log_message, 400

        message = request_json["message"]
        if "data" not in message:
            log_message = (
                "Invalid Pub/Sub message format: missing 'data' key in 'message'."
            )
            print(json.dumps({"message": log_message, "severity": "ERROR"}))
            return log_message, 400

        # Decode the base64-encoded data payload.
        data = base64.b64decode(message["data"]).decode("utf-8")
        payload = json.loads(data)
        target_url = payload.get("url")

        if not target_url:
            log_message = "URL not found in Pub/Sub message payload."
            print(json.dumps({"message": log_message, "severity": "ERROR"}))
            return log_message, 400

        # 2. Scrape the target website.
        headers = {"User-Agent": USER_AGENT}
        # Use a timeout to prevent the function from hanging indefinitely.
        response = requests.get(target_url, headers=headers, timeout=10)
        # Raise an HTTPError for bad responses (4xx or 5xx).
        response.raise_for_status()
        html_content = response.text

        # 3. Generate a deterministic filename from the URL for versioning.
        parsed_url = urlparse(target_url)
        # Create a path-like structure from the URL's netloc and path.
        # This ensures that pages from different domains are stored separately.
        filename = parsed_url.netloc + parsed_url.path

        # If the path is empty or just a '/', treat it as an index page.
        if filename.endswith("/"):
            filename += "index.html"
        # Handle root URL case where path is empty (e.g., http://example.com)
        elif not parsed_url.path:
            filename += "/index.html"
        # Ensure the filename has an extension if it's a "directory-like" URL without one
        elif not os.path.splitext(filename)[1]:
            filename += ".html"

        # 4. Upload the raw HTML to Cloud Storage.
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

    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # This catches errors from parsing the Pub/Sub message.
        log_message = f"Error decoding Pub/Sub message data: {e}"
        print(json.dumps({"message": log_message, "severity": "ERROR"}))
        return log_message, 400

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
    A CloudEvent-triggered function that processes a raw HTML file from GCS.
    If a configuration is found for the file's domain in `config.json`, it
    extracts next page and result links. Otherwise, it falls back to
    extracting h1, h2, and h3 headings.
    """
    # Load configuration from environment variables and config file
    processed_data_bucket = os.environ.get("PROCESSED_DATA_BUCKET")
    crawl_queue_topic = os.environ.get("CRAWL_QUEUE_TOPIC")
    config = load_config()

    if not processed_data_bucket:
        log_message = (
            "Configuration error: PROCESSED_DATA_BUCKET environment variable not set."
        )
        print(json.dumps({"message": log_message, "severity": "ERROR"}))
        return

    # Initialize variables for the error handler's context
    bucket_name = None
    file_name = None

    try:
        # 1. Parse event data to get the source bucket and file name.
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]

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

        # 2. Download the source HTML file.
        raw_bucket = storage_client.bucket(bucket_name)
        raw_blob = raw_bucket.blob(file_name)
        html_content = raw_blob.download_as_text()

        soup = BeautifulSoup(html_content, "html.parser")

        # 3. Determine processing strategy based on config.
        domain = file_name.split("/")[0]
        # Try to find a config for the exact domain (e.g., 'www.example.com').
        # If not found, try stripping 'www.' and looking again (e.g., 'example.com').
        domain_config = config.get(domain) or (
            config.get(domain.replace("www.", "", 1))
            if domain.startswith("www.")
            else None
        )

        if domain_config:
            # --- New Logic: Extract links based on config ---
            print(
                json.dumps(
                    {
                        "message": f"Found config for domain: {domain}. Extracting links.",
                        "severity": "INFO",
                    }
                )
            )
            base_url = f"https://{domain}"

            # Extract next page URL
            next_page_url = None
            next_page_selector = domain_config.get("next_page_selector")
            if next_page_selector:
                next_link_tag = soup.select_one(next_page_selector)
                if next_link_tag and next_link_tag.get("href"):
                    next_page_url = urljoin(base_url, next_link_tag["href"])

            # Extract result URLs
            result_urls = []
            result_link_selector = domain_config.get("result_link_selector")
            if result_link_selector:
                result_link_tags = soup.select(result_link_selector)
                for tag in result_link_tags:
                    if tag.get("href"):
                        result_urls.append(urljoin(base_url, tag["href"]))

            processed_data = {
                "source_file": file_name,
                "next_page_url": next_page_url,
                "result_urls": result_urls,
            }
            log_summary = {
                "links_found": len(result_urls),
                "next_page_found": bool(next_page_url),
            }

        else:
            # --- Fallback Logic: Extract all links ---
            print(
                json.dumps(
                    {
                        "message": f"No config for domain: {domain}. Fallback: extracting all links.",
                        "severity": "INFO",
                    }
                )
            )
            base_url = f"https://{domain}"
            result_urls = []
            for tag in soup.find_all("a"):
                if tag.get("href"):
                    result_urls.append(urljoin(base_url, tag["href"]))

            processed_data = {
                "source_file": file_name,
                "next_page_url": None,  # No concept of 'next' in fallback
                "result_urls": result_urls,
            }
            log_summary = {"links_found": len(result_urls), "next_page_found": False}
        # 4. Prepare and upload the processed data as JSON.
        json_data = json.dumps(processed_data, indent=2)

        # Create a corresponding .json filename.
        processed_file_name = f"{os.path.splitext(file_name)[0]}.json"

        processed_bucket = storage_client.bucket(processed_data_bucket)
        processed_blob = processed_bucket.blob(processed_file_name)
        processed_blob.upload_from_string(json_data, content_type="application/json")

        # 5. If a next page is found, publish it to the crawl queue topic.
        next_page_url = processed_data.get("next_page_url")
        if next_page_url and crawl_queue_topic:
            message_payload = {"url": next_page_url}
            message_data = json.dumps(message_payload).encode("utf-8")

            future = pubsub_publisher.publish(crawl_queue_topic, data=message_data)
            # future.result() # Optional: block until publish is complete

            print(
                json.dumps(
                    {
                        "message": f"Published next page to {crawl_queue_topic}",
                        "severity": "INFO",
                        "next_page_url": next_page_url,
                        "topic": crawl_queue_topic,
                    }
                )
            )

        success_message = (
            f"Successfully processed {file_name} and uploaded to "
            f"gs://{processed_data_bucket}/{processed_file_name}"
        )
        final_log_payload = {
            "message": success_message,
            "severity": "INFO",
            "source_file": file_name,
            "destination_bucket": processed_data_bucket,
            "destination_file": processed_file_name,
        }
        final_log_payload.update(log_summary)
        print(json.dumps(final_log_payload))

    except (
        google_exceptions.NotFound,
        google_exceptions.Forbidden,
        UnicodeDecodeError,
        KeyError,
    ) as e:
        # Handle common errors like file not found, permission issues,
        # non-text content, or malformed event data.
        # We try to get file_name and bucket_name if they exist for better context.
        file_name = file_name or "unknown"
        bucket_name = bucket_name or "unknown"
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
        file_name = file_name or "unknown"
        bucket_name = bucket_name or "unknown"
        log_payload = {
            "message": "An unexpected error occurred during data processing.",
            "severity": "ERROR",
            "error": str(e),
            "error_type": type(e).__name__,
            "source_file": file_name,
            "source_bucket": bucket_name,
        }
        print(json.dumps(log_payload))
