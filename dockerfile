FROM flink:1.20.0
COPY /flink-connector-jdbc-3.2.0-1.19.jar /opt/flink/lib/
COPY /flink-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
COPY /postgresql-42.7.4.jar /opt/flink/lib/
COPY /flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/

COPY /flink-json-1.20.0.jar /opt/flink/lib/
COPY /flink-python-1.20.0.jar /opt/flink/lib/
COPY /flink-python-1.20.0.jar /opt/flink/opt/
COPY /kafka-clients-3.4.0.jar /opt/flink/lib/
COPY /flink-sql-connector-postgres-cdc-3.1.0.jar /opt/flink/lib/

RUN set -ex; \
  apt-get update; \
  apt-get -y install python3 python3-pip python3-dev; \
  if [ ! -e /usr/bin/python ]; then ln -s /usr/bin/python3 /usr/bin/python; fi; \
  if [ ! -e /usr/bin/pip ]; then ln -s /usr/bin/pip3 /usr/bin/pip; fi

RUN set -ex; \
  apt-get update; \
  python -m pip install --upgrade pip; \
  pip install apache-flink kafka-python

ARG FLINK_VERSION=1.10.0