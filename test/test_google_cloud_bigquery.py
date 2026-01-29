from unittest import mock

import pandas as pd
import pytest

from conftest import require_module


def _get_module():
    require_module("google.cloud.bigquery", "pip install -r requirements.txt")
    require_module("google.api_core", "pip install -r requirements.txt")
    from utils import google_cloud_bigquery as gcbq

    return gcbq


def test_missing_project_id_raises():
    gcbq = _get_module()

    with mock.patch("utils.google_cloud_bigquery.os.getenv", return_value=None):
        with pytest.raises(ValueError):
            gcbq.load_to_bigquery(pd.DataFrame(), "dataset", "table")


def test_append_load_uses_load_job():
    gcbq = _get_module()
    with mock.patch("utils.google_cloud_bigquery.os.getenv", return_value="project"):
        with mock.patch("utils.google_cloud_bigquery.bigquery.Client") as mock_client_cls:
            with mock.patch("utils.google_cloud_bigquery.run_with_retry") as mock_retry:
                client = mock_client_cls.return_value
                dataset_ref = mock.Mock()
                table_ref = mock.Mock()
                dataset_ref.table.return_value = table_ref
                client.dataset.return_value = dataset_ref

                load_job = mock.Mock()
                load_job.result.return_value = None
                client.load_table_from_dataframe.return_value = load_job

                def passthrough(action, **kwargs):
                    return action()

                mock_retry.side_effect = passthrough

                df = pd.DataFrame([{"a": 1}])
                gcbq.load_to_bigquery(df, "dataset", "table", if_exists="append")

                client.load_table_from_dataframe.assert_called_once()
