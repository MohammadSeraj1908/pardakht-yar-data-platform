# Kafka Connect + Debezium PostgreSQL plugin on top of the official Apache Kafka image.
# This is the same plugin that the debezium/connect image ships, but ~600MB smaller
# and pinned explicitly.
FROM apache/kafka:3.8.0

ARG DEBEZIUM_VERSION=3.0.0.Final

USER root
RUN wget -q -O /tmp/debezium.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
 && mkdir -p /opt/kafka/plugins \
 && tar -xzf /tmp/debezium.tar.gz -C /opt/kafka/plugins \
 && rm /tmp/debezium.tar.gz \
 && chown -R appuser:appuser /opt/kafka/plugins
USER appuser

COPY connect-distributed.properties /opt/kafka/config/connect-distributed.properties

CMD ["/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/connect-distributed.properties"]
