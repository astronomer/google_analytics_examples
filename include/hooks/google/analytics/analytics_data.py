#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from typing import Optional, Sequence, Union

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

import pandas as pd


class GoogleAnalyticsDataHook(GoogleBaseHook):
    """
    Hook for Google Analytics Data.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "data_v1beta",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ):
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version
        self._client = None

    def get_conn(self):
        """
        Retrieves BetaAnalyticsDataClient if not yet created
        """
        if not self._client:
            self._client = BetaAnalyticsDataClient(
                credentials=self._get_credentials(), client_info=self.client_info
            )
        return self._client

    def run_report(self, property_id, dimensions, metrics, start_ds, end_ds):
        """
        Fetch report from the analytics data API.
        :param property_id: ID of the given property.
        :param dimensions: A list of the Dimensions, for detail of available dimensions ,
        ref https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/
        :param metrics: A list of the Metrics, for detail of available Metrics ,
        ref https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/
        :param start_ds: Start Date of the data range with either YYYY-MM-DD, yesterday, today, or NdaysAgo where N is a positive integer
        :param end_ds: End Date of the data range with either YYYY-MM-DD, yesterday, today, or NdaysAgo where N is a positive integer
        :return: Report
        """
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=self._get_dimensions_headers(dimensions),
            metrics=self._get_metrics_headers(metrics),
            date_ranges=[DateRange(start_date=start_ds, end_date=end_ds)],
        )
        client = self.get_conn()
        result = client.run_report(request)
        return result

    def get_report_df(self, property_id, dimensions, metrics, start_ds, end_ds):
        """
        Return report as DataFrame
        :param property_id: ID of the given property.
        :param dimensions: A list of the Dimensions, for detail of available dimensions ,
        ref https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/
        :param metrics: A list of the Metrics, for detail of available Metrics ,
        ref https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/
        :param start_ds: Start Date of the data range with either YYYY-MM-DD, yesterday, today, or NdaysAgo where N is a positive integer
        :param end_ds: End Date of the data range with either YYYY-MM-DD, yesterday, today, or NdaysAgo where N is a positive integer
        :return: Report DataFrame
        """
        response = self.run_report(property_id, dimensions, metrics, start_ds, end_ds)
        columns = []
        for dimension in response.dimension_headers:
            columns.append(dimension.name)
        for metric in response.metric_headers:
            columns.append(metric.name)

        rows = []
        for row in response.rows:
            row_values = []
            for value in row.dimension_values:
                row_values.append(value.value)
            for value in row.metric_values:
                row_values.append(value.value)
            rows.append(row_values)
        df = pd.DataFrame(rows, columns=columns)
        # The date we get from GA Data api are with the YYYYMMDD format.
        # https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/
        # Format the date column if there is a date column.
        if df.columns.__contains__("date"):
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime(
                "%Y-%m-%d"
            )
        if df.columns.__contains__("firstSessionDate"):
            df["date"] = pd.to_datetime(
                df["firstSessionDate"], format="%Y%m%d"
            ).dt.strftime("%Y-%m-%d")
        return df

    def _get_dimensions_headers(self, dimensions):
        """
        Generate a list of Dimension objects
        :param dimensions: List of the dimensions
        :return: List of the Dimension objects
        """
        dimension_list = []
        for dimension in dimensions:
            dimension_list.append(Dimension(name="{0}".format(dimension)))
        return dimension_list

    def _get_metrics_headers(self, metrics):
        """
        Generate a list of Metrics objects
        :param dimensions: List of the metrics
        :return: List of the Meetrics objects
        """
        metric_list = []
        for metric in metrics:
            metric_list.append(Metric(name="{0}".format(metric)))
        return metric_list
