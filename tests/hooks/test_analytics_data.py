import os, sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../../")

import mock
import pytest

import pandas as pd

from include.hooks.google.analytics.analytics_data import GoogleAnalyticsDataHook
from google.analytics.data_v1beta.types import Dimension, Metric, Row

GCP_CONN_ID = "gcp_conn_id"
API_VERSION = "data_v1beta"
CLIENT_REPORT = {"key": "value"}
PROJECT = "astronomer-cloud"
SCOPE = "https://www.googleapis.com/auth/analytics.readonly, https://www.googleapis.com/auth/cloud-platform"
SECRET = "SECRET"
EXTRAS = {
    "extra__google_cloud_platform__keyfile_dict": SECRET,
    "extra__google_cloud_platform__project": PROJECT,
    "extra__google_cloud_platform__scope": SCOPE,
}
START_DS = "1970-01-01"
END_DS = "yesterday"
PROPERTY_ID = "GA_Property_Id"
DIMENSIONS = ["country", "date"]
METRICS = ["activeUsers", "newUsers", "sessions"]
dimensionsHeaders = [Dimension(name="country"), Dimension(name="date")]
metricsHeaders = [
    Metric(name="activeUsers"),
    Metric(name="newUsers"),
    Metric(name="sessions"),
]
rows = [
    Row(
        {
            "dimension_values": [{"value": "United States"}, {"value": "20211206"}],
            "metric_values": [{"value": "133"}, {"value": "95"}, {"value": "79"}],
        }
    ),
    Row(
        {
            "dimension_values": [{"value": "United States"}, {"value": "20211207"}],
            "metric_values": [{"value": "153"}, {"value": "105"}, {"value": "88"}],
        }
    ),
]

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
def mock_hook():
    with mock.patch(
        "airflow.hooks.base_hook.BaseHook.get_connection"
    ) as conn, mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id",
        return_value=(SECRET, None),
    ), mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.client_info",
        new_callable=mock.PropertyMock,
        return_value="CLIENT_INFO",
    ), mock.patch(
        "google.analytics.data_v1beta.BetaAnalyticsDataClient.__init__",
        return_value=None,
    ), mock.patch(
        "google.analytics.data_v1beta.BetaAnalyticsDataClient.run_report"
    ) as run_report:
        hook = GoogleAnalyticsDataHook(gcp_conn_id=GCP_CONN_ID, api_version=API_VERSION)
        conn.return_value.extra_dejson = EXTRAS
        run_report.return_value.dimension_headers = dimensionsHeaders
        run_report.return_value.metric_headers = metricsHeaders
        run_report.return_value.rows = rows
        yield hook


class TestGoogleAnalyticsDataHook:
    def test_init(self, mock_hook):
        assert mock_hook.api_version == API_VERSION

    def test_get_conn(self, mock_hook):
        assert mock_hook._client is None
        client = mock_hook.get_conn()
        assert client is not None

    def test_get_conn_called_twice(self, mock_hook):
        client1 = mock_hook.get_conn()
        client2 = mock_hook.get_conn()
        client3 = mock_hook.get_conn()
        assert client1 == client2
        assert client1 == client3

    def test_get_report(self, mock_hook):
        report = mock_hook.run_report(
            PROPERTY_ID, DIMENSIONS, METRICS, START_DS, END_DS
        )
        assert report.dimension_headers == dimensionsHeaders
        assert report.metric_headers == metricsHeaders
        assert report.rows == rows
        mock_hook._client.run_report.assert_called_once()

    def test_get_report_df(self, mock_hook):
        report_df = mock_hook.get_report_df(
            PROPERTY_ID, DIMENSIONS, METRICS, START_DS, END_DS
        )
        assert report_df.equals(REPORT_DF)
        mock_hook._client.run_report.assert_called_once()

    def test_get_dimensions_headers(self, mock_hook):
        dimensions_headers = mock_hook._get_dimensions_headers(DIMENSIONS)
        assert dimensions_headers == dimensionsHeaders

    def test_get_metrics_headers(self, mock_hook):
        metrics_headers = mock_hook._get_metrics_headers(METRICS)
        assert metrics_headers == metricsHeaders
