# FROM apache/airflow:2.8.1-python3.10
# ENV PYTHONPATH "$AIRFLOW_HOME"
# COPY requirements.txt /requirements.txt
# # COPY .ENV /opt/airflow/.ENV
# RUN pip install --user --upgrade pip
# RUN pip install --no-cache-dir --user -r /requirements.txt


FROM apache/airflow:2.8.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq
COPY airflow_requirements.txt .

USER ${AIRFLOW_UID:-50000}:0
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r airflow_requirements.txt

USER root

WORKDIR $AIRFLOW_HOME

