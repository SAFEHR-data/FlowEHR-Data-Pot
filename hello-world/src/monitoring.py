#  Copyright (c) University College London Hospitals NHS Foundation Trust
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import os
from typing import Any, Dict, List, Optional, Union

from opencensus.ext.azure import metrics_exporter
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.stats import aggregation, stats
from opencensus.stats.measure import MeasureFloat, MeasureInt
from opencensus.stats.view import View
from opencensus.tags import tag_key, tag_map
from opencensus.trace import config_integration
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.tracer import Tracer


def telemetry_processor_callback_function(envelope: Any):
    envelope.tags["ai.cloud.role"] = "databricks"


class ExceptionTracebackFilter(logging.Filter):
    """
    If a record contains 'exc_info', it will only show in the 'exceptions'
    section of Application Insights without showing in the 'traces' section.
    In order to show it also in the 'traces' section, we need another log
    that does not contain 'exc_info'.
    """

    def filter(self, record):
        if record.exc_info:
            logger = logging.getLogger(record.name)
            _, exception_value, _ = record.exc_info
            message = f"{record.getMessage()}\nException message: '{exception_value}'"
            print(f"{record.getMessage()}\nException message: '{exception_value}'")
            logger.log(record.levelno, message)

        return True


def initialize_logging(
    logging_level: int = logging.INFO,
    export_interval_seconds: float = 5.0,
    correlation_id: Optional[str] = None,
) -> logging.LoggerAdapter:
    """
    Adds the Application Insights handler for the root logger and sets
    the given logging level. Creates and returns a logger adapter that integrates
    the correlation ID, if given, to the log messages.
    :param logging_level: The logging level to set e.g., logging.WARNING.
    :param correlation_id: Optional. The correlation ID that is passed on
    to the operation_Id in App Insights.
    :returns: A newly created logger adapter.
    """
    logger = logging.getLogger()

    try:
        azurelog_handler = AzureLogHandler(export_interval=export_interval_seconds)
        azurelog_handler.add_telemetry_processor(telemetry_processor_callback_function)
        azurelog_handler.addFilter(ExceptionTracebackFilter())
        logger.addHandler(azurelog_handler)
    except ValueError as e:
        logger.error(f"Failed to set Application Insights logger handler: {e}")

    config_integration.trace_integrations(["logging"])
    Tracer(sampler=AlwaysOnSampler())
    logger.setLevel(logging_level)

    extra = {}

    if correlation_id:
        extra = {"traceId": correlation_id}

    adapter = logging.LoggerAdapter(logger, extra)
    adapter.debug(f"Logger adapter initialized with extra: {extra}")

    return adapter


def create_and_send_metric(
    value: Union[int, float],
    tags: Dict[str, str],
    name: str,
    description: str,
    view_prefix: str,
    unit: str,
    tag_keys: List[str],
    metric_type: Union[MeasureInt, MeasureFloat],
    aggregation: type = aggregation.SumAggregation,
    export_interval_seconds: float = 1.0,
) -> None:
    """
    This method creates a metric measure, a corresponding View with columns and
    aggregations provided, registers it with the Azure metrics exporter, and sends
    the value of the metric. It initializes the exporter using the
    APPLICATIONINSIGHTS_CONNECTION_STRING env variable. If it isn't set, it will
    exit silently.

    :param value: Value of the metric to send.
    :type value: int, float
    :param tags: Tags to be associated with the metric.
    :type tags: dict
    :param name: Name of the metric.
    :type name: str
    :param description: Description of the metric.
    :type description: str
    :param view_prefix: Prefix to use for the Metric view.
    :type view_prefix: str
    :param unit: Description of the unit, which must follow
                 https://unitsofmeasure.org/ucum.
    :type unit: str
    :param tag_keys: Tag keys to aggregate on for the view created.
    :type tag_keys: list
    :param metric_type: Type to use for the created metric, can be either
                        opencensus.stats.measure.MeasureInt or
                        opencensus.stats.measure.MeasureFloat.
    :type metric_type: MeasureInt, MeasureFloat
    :param aggregation: Type of aggregation as described in
                        https://opencensus.io/stats/view/#aggregations.
    :type aggregation: Aggregation
    :param export_interval_seconds: How often the metrics will be exported,
                                    the default is every 1 second.
    :type export_interval_seconds: int
    """

    if "APPLICATIONINSIGHTS_CONNECTION_STRING" not in os.environ:
        logging.warn("APPLICATIONINSIGHTS_CONNECTION_STRING is not set, exiting")
        return

    metric_measure = metric_type(name=name, description=description, unit=unit)
    key_methods = [tag_key.TagKey(key) for key in tag_keys]

    rows_written_view = View(
        name=f"{view_prefix}/{name}",
        description=description,
        columns=key_methods,
        measure=metric_measure,
        aggregation=aggregation(),
    )

    view_manager = stats.stats.view_manager
    view_manager.register_view(rows_written_view)

    mmap = stats.stats.stats_recorder.new_measurement_map()
    tagmap = tag_map.TagMap()
    for tag_name, tag_value in tags.items():
        tagmap.insert(tag_name, tag_value)

    exporter = metrics_exporter.new_metrics_exporter(
        export_interval=export_interval_seconds
    )
    view_manager.register_exporter(exporter)

    mmap.measure_int_put(metric_measure, value)
    mmap.record(tagmap)
