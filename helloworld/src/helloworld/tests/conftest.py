import os
import time
from pathlib import Path
from typing import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session() -> Iterator[SparkSession]:
    # Used to ensure the tested python environment is using UTC like the Spark
    # session so that there are no differences between timezones.
    os.environ["TZ"] = "UTC"
    time.tzset()

    session = (
        SparkSession.builder.master("local[4]")
        .config("spark.jars", Path(__file__).parent / "postgresql-42.5.2.jar")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
