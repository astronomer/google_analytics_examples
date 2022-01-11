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
"""
This module contains Google Analytics Data to GCS operators.
"""
import logging
import tempfile
from typing import Dict, Iterable

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from include.hooks.google.analytics.analytics_data import GoogleAnalyticsDataHook


class GAToGCSOperator(BaseOperator):
    """
    Get Google analytics data report and save reports to GCS.

    .. seealso::
        Google Analytics Data API (GA4): https://developers.google.com/analytics/devguides/reporting/data/v1
        GA4 Query Explorer: https://ga-dev-tools.web.app/ga4/query-explorer/
    :param property_id: ID of the given property.
    :param gcs_bucket_name: Bucket Name of GCS.
    :param ga_conn_id: The connection ID to use when fetching Google Analytics Data API connection info.
    :param gcs_conn_id: The connection ID to use when fetching GCS connection info.
    :param gcs_dest_path: Path to store report to.
    :param dimensions: A list of the Dimensions, for detail, see also https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/
    :param metrics: A list of the Metrics, for detail, see also https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/
    :param start_ds: Start Date of the data range with either YYYY-MM-DD, yesterday, today, or NdaysAgo where N is a positive integer
    :param end_ds: End Date of the data range with either YYYY-MM-DD, yesterday, today, or NdaysAgo where N is a positive integer
    :param kwargs:

    """

    template_fields: Iterable[str] = (
        "dimensions",
        "metrics",
        "start_ds",
        "end_ds",
        "gcs_dest_path",
    )

    def __init__(
        self,
        property_id,
        gcs_bucket_name,
        ga_conn_id: str = "google_analytic_data_default",
        gcs_conn_id: str = "google_cloud_default",
        gcs_dest_path="out.csv",
        dimensions=[],
        metrics=[],
        start_ds="1970-01-01",
        end_ds="yesterday",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.start_ds = start_ds
        self.end_ds = end_ds
        self.property_id = property_id
        self.dimensions = dimensions
        self.metrics = metrics
        self.gcs_dest_path = gcs_dest_path
        self.gcs_conn_id = gcs_conn_id
        self.gcs_bucket_name = gcs_bucket_name
        self.ga_conn_id = ga_conn_id
        self.logger = logging.getLogger("airflow.task")

    def _upload_to_gcs(
        self,
        gcs_hook,
        ga_data_hook,
        property_id,
        gcs_dest_path,
        dimensions,
        metrics,
        start_ds,
        end_ds,
    ):
        self.logger.info(property_id)
        df = self._get_report_df(
            ga_data_hook=ga_data_hook,
            property_id=property_id,
            dimensions=dimensions,
            metrics=metrics,
            start_ds=start_ds,
            end_ds=end_ds,
        )
        self._upload_df_to_gcs_as_csv(
            gcs_hook=gcs_hook, df=df, gcs_dest_path=gcs_dest_path
        )

    def _get_report_df(
        self, ga_data_hook, property_id, dimensions, metrics, start_ds, end_ds
    ):
        df = ga_data_hook.get_report_df(
            property_id, dimensions, metrics, start_ds, end_ds
        )
        return df

    def _upload_df_to_gcs_as_csv(self, gcs_hook, df, gcs_dest_path):
        with tempfile.NamedTemporaryFile() as in_mem_file:
            df.to_csv(in_mem_file, index=False)
            gcs_hook.upload(
                bucket_name=self.gcs_bucket_name,
                object_name=gcs_dest_path,
                filename=in_mem_file.name,
            )

    def execute(self, context: Dict) -> None:
        gcs_hook = GCSHook(gcp_conn_id=self.gcs_conn_id)
        ga_data_hook = GoogleAnalyticsDataHook(gcp_conn_id=self.ga_conn_id)
        self._upload_to_gcs(
            gcs_hook=gcs_hook,
            ga_data_hook=ga_data_hook,
            property_id=self.property_id,
            gcs_dest_path=self.gcs_dest_path,
            start_ds=self.start_ds,
            end_ds=self.end_ds,
            dimensions=self.dimensions,
            metrics=self.metrics,
        )
