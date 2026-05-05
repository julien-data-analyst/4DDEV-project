FROM apache/spark:3.5.1-scala2.12-java17-python3-r-ubuntu

USER root

COPY scripts/ /opt/spark/scripts/

ENV SPARK_EXTRA_CLASSPATH="/opt/spark/jars/*"