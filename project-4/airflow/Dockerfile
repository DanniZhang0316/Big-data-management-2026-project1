FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME path differs between arm64 and amd64 images. Resolve it dynamically.
RUN echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /etc/profile.d/java.sh
ENV PATH="/usr/lib/jvm/default-java/bin:${PATH}"

USER airflow

# install ROOT requirements first
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# then airflow-specific requirements
COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt