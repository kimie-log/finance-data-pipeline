from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions

from utils.logger import logger
from utils.retry import run_with_retry

RETRYABLE_EXCEPTIONS = (
    gcp_exceptions.ServiceUnavailable,
    gcp_exceptions.DeadlineExceeded,
    gcp_exceptions.InternalServerError,
    gcp_exceptions.TooManyRequests,
    gcp_exceptions.Aborted,
    gcp_exceptions.GatewayTimeout,
)

def upload_file(bucket_name, source_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    try:
        run_with_retry(
            lambda: blob.upload_from_filename(str(source_path)),
            action_name=f"GCS upload {bucket_name}/{destination_blob_name}",
            retry_exceptions=RETRYABLE_EXCEPTIONS,
        )
    except gcp_exceptions.NotFound:
        logger.error(
            "GCS bucket not found: %s (upload %s)",
            bucket_name,
            destination_blob_name,
        )
        raise
    except gcp_exceptions.Forbidden as exc:
        logger.error(
            "GCS permission denied for bucket %s: %s",
            bucket_name,
            exc,
        )
        raise

def download_file(bucket_name, destination_path, source_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    try:
        run_with_retry(
            lambda: blob.download_to_filename(str(destination_path)),
            action_name=f"GCS download {bucket_name}/{source_blob_name}",
            retry_exceptions=RETRYABLE_EXCEPTIONS,
        )
    except gcp_exceptions.NotFound:
        logger.error(
            "GCS object not found: %s/%s",
            bucket_name,
            source_blob_name,
        )
        raise
    except gcp_exceptions.Forbidden as exc:
        logger.error(
            "GCS permission denied for bucket %s: %s",
            bucket_name,
            exc,
        )
        raise
