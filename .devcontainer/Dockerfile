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
# limitations under the License.
# You can pick any Debian/Ubuntu-based image. 😊
FROM mcr.microsoft.com/vscode/devcontainers/python:3.10

COPY library-scripts/*.sh /tmp/library-scripts/

# [Option] Install zsh
ARG INSTALL_ZSH="true"
# [Option] Upgrade OS packages to their latest versions
ARG UPGRADE_PACKAGES="false"

# Install needed packages and setup non-root user. Use a separate RUN statement to add your own dependencies.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && bash /tmp/library-scripts/common-debian.sh "${INSTALL_ZSH}" "${USERNAME}" "${USER_UID}" "${USER_GID}" "${UPGRADE_PACKAGES}" "true" "true" \ 
    && apt-get install -y graphviz --no-install-recommends \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

ENTRYPOINT [ "/usr/local/share/docker-init.sh" ]
CMD [ "sleep", "infinity" ]

ARG PRECOMMIT_VERSION="2.21.0"
RUN python3 -m pip install pre-commit=="${PRECOMMIT_VERSION}" --no-cache-dir

# Install Python dependencies for data transform pipeline
ARG SPARK_VERSION="3.3.1"
RUN python3 -m pip install pyspark==3.3.1 build numpy databricks-cli --no-cache-dir

# Get the local Spark installed to be able to run unit tests
# Borrowed from https://github.com/beandrad/pyspark-sample/blob/main/.devcontainer/Dockerfile#L36
RUN mkdir -p /tmp/docker-downloads \
    && sudo apt-get update && sudo apt-get -y install openjdk-11-jdk wget \
    && wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -O /tmp/docker-downloads/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xvzf /tmp/docker-downloads/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm -rf /tmp/docker-downloads
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin