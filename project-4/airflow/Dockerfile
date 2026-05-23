FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# install ROOT requirements first
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# then airflow-specific requirements
COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt