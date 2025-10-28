#FROM astrocrpublic.azurecr.io/runtime:3.1-1
FROM quay.io/astronomer/astro-runtime:13.1.0
#-ubi9-python-3.11
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
ENV AIRFLOW__COSMOS__ENRICH_LOGGING = "True"

# Create a venv for dbt in scheduler image and install required packages
# Note: astro-runtime already provides Python; we add system deps if needed and create a dedicated venv
#USER root
#
## Install minimal build deps for python wheels if needed (kept lightweight)
#RUN microdnf install -y gcc gcc-c++ make python3-devel openssl-devel \
#    && microdnf clean all \
#    && rm -rf /var/cache/dnf
#
## Switch to astro to create and own the venv
#USER astro
#ENV HOME=/usr/local/airflow

# Create venv and install dbt + duckdb as astro user
RUN python3 -m venv /usr/local/airflow/dbt_venv \
    && /usr/local/airflow/dbt_venv/bin/pip install --upgrade pip \
    && /usr/local/airflow/dbt_venv/bin/pip install \
        duckdb==1.4.1 \
        dbt-core==1.10.13 \
        dbt-duckdb \
        dbt-postgres \
        dbt-glue \
    && rm -rf /usr/local/airflow/.cache/pip

