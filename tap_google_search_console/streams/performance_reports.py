from singer.logger import get_logger
from typing import Optional
from copy import deepcopy


from .abstract import IncrementalTableStream

LOGGER = get_logger()


class PerformanceReportCustom(IncrementalTableStream):
    """Class Representing the `performance_report_custom` Stream."""

    tap_stream_id = "performance_report_custom"
    key_properties = ["site_url", "search_type", "date", "dimensions_hash_key"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "auto"}
    dimension_list = ["date", "country", "device", "page", "query"]


class PerformanceReportDate(IncrementalTableStream):
    """Class Representing the `performance_report_date` Stream."""

    tap_stream_id = "performance_report_date"
    key_properties = ["site_url", "search_type", "date"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "byProperty", "dimensions": ["date"]}


class PerformanceReportCountry(IncrementalTableStream):
    """Class Representing the `performance_report_country` Stream."""

    tap_stream_id = "performance_report_country"
    key_properties = ["site_url", "search_type", "date", "country"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "byProperty", "dimensions": ["date", "country"]}


class PerformanceReportSearchAppearance(IncrementalTableStream):
    """Class Representing the `performance_report_search_appearance` Stream."""

    tap_stream_id = "performance_report_search_appearance"
    key_properties = ["site_url", "search_type", "date", "page"]
    valid_replication_keys = ("date",)


    body_params = {"aggregationType": "byProperty","type" : "web", "dimensions": ["date", "page"],
  "dimensionFilterGroups": [
    {
      "filters": [
        {
          "dimension": "searchAppearance",
          "operator": "equals",
          "expression": "{search_appearance}"
        }
      ]
    }
  ]}
    def get_search_appearances(self):
        return self.config.get("search_appearences", "").replace(" ", "").split(",")
    
    
    def get_records(self, state: dict, schema: dict, stream_metadata: dict) -> None:
        """Starts extracting data for each site_url and each search_appearance."""
        search_appearances = self.get_search_appearances()

        for site in self.get_site_url():
            for search_appearance in search_appearances:
                LOGGER.info(f"Starting Sync for Stream {self.tap_stream_id}, Site {site}, SearchAppearance {search_appearance}")

                # Create a fresh copy of body_params for each iteration
                current_body_params = deepcopy(self.body_params)
                current_body_params["dimensionFilterGroups"][0]["filters"][0]["expression"] = search_appearance
                self.body_params = current_body_params
                self.get_records_for_site(site, state, schema, stream_metadata,search_appearance)

                LOGGER.info(f"Finished Sync for Stream {self.tap_stream_id}, Site {site}, SearchAppearance {search_appearance}")  
  


class PerformanceReportDevices(IncrementalTableStream):
    """Class Representing the `performance_report_device` Stream."""

    tap_stream_id = "performance_report_device"
    key_properties = ["site_url", "search_type", "date", "device"]
    # Excluding discover sub_type since Requests for Discover cannot be grouped by device.
    # this also overwrites the sub_types attribute declared for class IncrementalTableStream
    # in abstract.py
    sub_types = ["googleNews", "image", "news", "video", "web"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "byProperty", "dimensions": ["date", "device"]}


class PerformanceReportPage(IncrementalTableStream):
    """Class Representing the `performance_report_page` Stream."""

    tap_stream_id = "performance_report_page"
    key_properties = ["site_url", "search_type", "date", "page"]
    valid_replication_keys = ("date", "page")

    body_params = {"aggregationType": "byPage", "dimensions": ["date", "page"]}


class PerformanceReportQuery(IncrementalTableStream):
    """Class Representing the `performance_report_query` Stream."""

    tap_stream_id = "performance_report_query"
    key_properties = ["site_url", "search_type", "date", "query"]
    # Excluding discover and googleNews since query seems to be an invalid argument while
    # grouping data for discover and googleNews
    # this also overwrites the sub_types attribute declared for class IncrementalTableStream
    # in abstract.py
    sub_types = ["image", "news", "video", "web"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "byProperty", "dimensions": ["date", "query"]}
