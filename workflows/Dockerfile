FROM apache/airflow:2.10.0-python3.11

ENV AIRFLOW_HOME=/opt/airflow
# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER root

# Update the package list, install required packages, and clean up
# RUN apt-get update && \
#     apt-get install -y gcc python3-dev openjdk-11-jdk wget && \
#     apt-get clean

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev python3-dev default-jdk wget curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

USER airflow
RUN pip install --upgrade pip

# Use pip's constraint mode to avoid backtracking
RUN pip install --no-cache-dir --use-pep517 --constraint=https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.11.txt -r requirements.txt 

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER 0

ARG CLOUD_SDK_VERSION=446.0.0
ENV GCLOUD_HOME=/opt/google-cloud-sdk

# ARG dbt_core_ref=dbt-core@v1.0.1
# ARG dbt_postgres_ref=dbt-core@v1.0.1
# ARG dbt_redshift_ref=dbt-redshift@v1.0.0
# ARG dbt_bigquery_ref=dbt-bigquery@v1.0.0
# ARG dbt_spark_ref=dbt-spark@v1.0.0
# # special case args
# ARG dbt_spark_version=all
# ARG dbt_third_party

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID