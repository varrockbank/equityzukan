FROM apache/airflow:2.8.0-python3.11

USER root

# Install Java JDK for PySpark and procps for process management
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python dependencies
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
COPY --chown=airflow:root src/ /opt/airflow/src/

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
