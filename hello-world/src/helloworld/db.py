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
import abc
import logging
from dataclasses import dataclass

from helloworld.constants import PIPELINE_NAME
from helloworld.monitoring import create_and_send_metric
from opencensus.stats.measure import MeasureInt
from pyspark.sql import DataFrame, SparkSession


@dataclass
class DatabaseConfiguration:
    host: str
    port: int
    database: str
    user: str
    password: str
    driver: str | None

    @abc.abstractmethod
    def url(self) -> str:
        pass


@dataclass
class SqlServerConfiguration(DatabaseConfiguration):
    def url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"database={self.database};"
            "encrypt=true;"
            "trustServerCertificate=false;"
            "loginTimeout=30;"
            "Authentication=ActiveDirectoryServicePrincipal"
        )


def feature_store_config(spark_session: SparkSession) -> DatabaseConfiguration:
    return SqlServerConfiguration(
        host=spark_session.conf.get("spark.secret.feature-store-fqdn"),
        port=1433,
        database=spark_session.conf.get("spark.secret.feature-store-database"),
        user=spark_session.conf.get("spark.secret.feature-store-app-id"),
        password=spark_session.conf.get("spark.secret.feature-store-app-secret"),
        driver=None,  # Use the default driver.
    )


def save_table(config: DatabaseConfiguration, df: DataFrame, table_name: str) -> None:
    writer = (
        df.write.format("jdbc")
        .mode("append")
        .option("url", config.url())
        .option("dbtable", table_name)
        .option("user", config.user)
        .option("password", config.password)
    )

    if config.driver is not None:
        writer = writer.option("driver", config.driver)

    writer.save()


def save_feature_store_table(
    config: DatabaseConfiguration, df: DataFrame, table_name: str
) -> None:
    # Name expected by FlowEHR in order to create dashboards automatically
    METRIC_NAME = "rows_inserted"

    create_and_send_metric(
        value=df.count(),
        tags={"table_name": table_name},
        name=METRIC_NAME,
        view_prefix=PIPELINE_NAME,
        description="Number of rows inserted into the database",
        unit="1",
        tag_keys=["table_name"],
        metric_type=MeasureInt,
    )

    save_table(config, df, table_name)

    logging.info(f"Written {df.count()} rows into table {table_name}")
