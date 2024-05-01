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

COPY requirements.txt .
# USER $AIRFLOW_UID
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

