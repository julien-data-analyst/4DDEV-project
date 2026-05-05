FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# sinon les commandes airflow ne sont plus dans le PATH
USER airflow

# S'assurer que le PATH local de airflow est bien inclus
ENV PATH="/home/airflow/.local/bin:${PATH}"

# COPY du code
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY config/ /opt/airflow/config/
COPY plugins/ /opt/airflow/plugins/