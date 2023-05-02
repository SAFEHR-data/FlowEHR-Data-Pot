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
from typing import Optional

from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.trace import config_integration
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.tracer import Tracer


def telemetry_processor_callback_function(envelope):
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
    logging_level: int,
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
