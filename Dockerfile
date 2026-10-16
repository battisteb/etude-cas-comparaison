# Environnement Linux pour PySpark : Python 3.12 + Java 17 + PySpark 4.2.0 + Jupyter (même image que le TP2).
FROM python:3.12-slim-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN pip install --no-cache-dir pyspark==4.2.0 pandas==2.3.3 psutil==7.1.3 pytest==9.1.1
WORKDIR /work
RUN pip install --no-cache-dir jupyter==1.1.1 matplotlib==3.10.6 nbconvert==7.16.6
