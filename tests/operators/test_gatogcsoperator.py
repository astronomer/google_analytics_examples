import os, sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../../")

import mock
import pytest
import pandas as pd

from include.operators.google.analytics.gatogcsoperator import GAToGCSOperator
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import Row


TASK_ID = "test-gcs-to-gcs-operator"
START_DS = "1970-01-01"
END_DS = "yesterday"
PROPERTY_ID = "GA_Property_Id"
GCS_CONN_ID = GA_CONN_ID = "gcs_conn_id"
GCS_BUCKET = "test-bucket"
GCS_FILE_PATH = "test-path"
DIMENSIONS = ["country", "date"]
METRICS = ["activeUsers", "newUsers", "sessions"]

report_data = [
    {
        "country": "United States",
        "date": "2021-12-06",
        "activeUsers": "133",
        "newUsers": "95",
        "sessions": "79",
    },
    {
        "country": "United States",
        "date": "2021-12-07",
        "activeUsers": "153",
        "newUsers": "105",
        "sessions": "88",
    },
]
REPORT_DF = pd.DataFrame(
    data=report_data, columns=["country", "date", "activeUsers", "newUsers", "sessions"]
)


@pytest.fixture()
def mock_operator():
    with mock.patch(
        "include.hooks.google.analytics.analytics_data.GoogleAnalyticsDataHook"
    ) as ga_data_hook, mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection"
    ), mock.patch(
        "airflow.providers.google.cloud.hooks.gcs.GCSHook"
    ) as gcs_hook:
        ga_to_gcs = GAToGCSOperator(
            task_id=TASK_ID,
            start_ds=START_DS,
            property_id=PROPERTY_ID,
            dimensions=DIMENSIONS,
            metrics=METRICS,
            gcs_dest_path=GCS_FILE_PATH,
            ga_conn_id=GA_CONN_ID,
            gcs_conn_id=GCS_CONN_ID,
            gcs_bucket_name=GCS_BUCKET,
        )
        yield ga_to_gcs


@pytest.fixture()
def mock_ga_data_hook():
    with mock.patch(
        "include.hooks.google.analytics.analytics_data.GoogleAnalyticsDataHook"
    ) as ga_data_hook:
        yield ga_data_hook


@pytest.fixture()
def mock_gcs_hook():
    with mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook") as gcs_hook:
        yield gcs_hook


class TestGAToGCSOperator:
    def test_init(self, mock_operator):
        assert mock_operator.task_id == TASK_ID
        assert mock_operator.start_ds == START_DS
        assert mock_operator.property_id == PROPERTY_ID
        assert mock_operator.dimensions == DIMENSIONS
        assert mock_operator.gcs_dest_path == GCS_FILE_PATH
        assert mock_operator.ga_conn_id == GA_CONN_ID
        assert mock_operator.gcs_bucket_name == GCS_BUCKET

    def test_get_report_to_df(self, mock_ga_data_hook, mock_operator):
        mock_operator._get_report_df(
            mock_ga_data_hook, PROPERTY_ID, DIMENSIONS, METRICS, START_DS, END_DS
        )
        mock_ga_data_hook.get_report_df.assert_called_once_with(
            PROPERTY_ID, DIMENSIONS, METRICS, START_DS, END_DS
        )

    def test__upload_df_to_gcs_as_csv(self, mock_gcs_hook, mock_operator):
        mock_operator._upload_df_to_gcs_as_csv(
            gcs_hook=mock_gcs_hook, df=REPORT_DF, gcs_dest_path=GCS_FILE_PATH
        )
        mock_gcs_hook.upload.assert_called_once()

    def test__upload_to_gcs(self, mock_gcs_hook, mock_ga_data_hook, mock_operator):
        mock_operator._upload_to_gcs(
            gcs_hook=mock_gcs_hook,
            ga_data_hook=mock_ga_data_hook,
            property_id=PROPERTY_ID,
            gcs_dest_path=GCS_FILE_PATH,
            dimensions=DIMENSIONS,
            metrics=METRICS,
            start_ds=START_DS,
            end_ds=END_DS,
        )
        mock_ga_data_hook.get_report_df.assert_called_once()
        mock_gcs_hook.upload.assert_called_once()

    @mock.patch(
        "include.operators.google.analytics.gatogcsoperator.GAToGCSOperator._upload_to_gcs"
    )
    def test_execute(self, ga_to_gcs_upload_to_gcs, mock_operator):
        mock_operator.execute(None)
        mock_operator._upload_to_gcs.assert_called_once()
