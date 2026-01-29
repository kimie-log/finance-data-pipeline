from unittest import mock

import pytest

from conftest import require_module


def _get_modules():
    require_module("google.cloud.storage", "pip install -r requirements.txt")
    require_module("google.api_core", "pip install -r requirements.txt")
    from google.api_core import exceptions as gcp_exceptions
    from utils import google_cloud_storage as gcs

    return gcp_exceptions, gcs


def test_upload_uses_retry():
    _, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.storage.Client") as mock_client:
            blob = mock.Mock()
            bucket = mock.Mock()
            bucket.blob.return_value = blob
            mock_client.return_value.bucket.return_value = bucket

            gcs.upload_file("bucket", "/tmp/source", "dest")

            mock_retry.assert_called_once()
            _, kwargs = mock_retry.call_args
            assert kwargs["action_name"] == "GCS upload bucket/dest"


def test_upload_not_found_logs():
    gcp_exceptions, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.logger") as mock_logger:
            mock_retry.side_effect = gcp_exceptions.NotFound("missing")

            with pytest.raises(gcp_exceptions.NotFound):
                gcs.upload_file("bucket", "/tmp/source", "dest")

            mock_logger.error.assert_called()


def test_download_uses_retry():
    _, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.storage.Client") as mock_client:
            blob = mock.Mock()
            bucket = mock.Mock()
            bucket.blob.return_value = blob
            mock_client.return_value.bucket.return_value = bucket

            gcs.download_file("bucket", "/tmp/dest", "source")

            mock_retry.assert_called_once()
            _, kwargs = mock_retry.call_args
            assert kwargs["action_name"] == "GCS download bucket/source"


def test_download_not_found_logs():
    gcp_exceptions, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.logger") as mock_logger:
            mock_retry.side_effect = gcp_exceptions.NotFound("missing")

            with pytest.raises(gcp_exceptions.NotFound):
                gcs.download_file("bucket", "/tmp/dest", "source")

            mock_logger.error.assert_called()
