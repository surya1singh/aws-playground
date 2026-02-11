# MSK → Spark Structured Streaming → S3 Parquet + SNS Alerts

Build a near-real-time streaming pipeline that ingests clickstream + order events from Kafka (MSK), enforces schema, handles duplicates and late events, writes curated Parquet to S3 partitioned by event date/hour, and sends SNS alerts when the error rate or event drop rate spikes.




For testing, I used Spark and Kafka on Docker.
```
docker network create kafka-net
```
```
docker run -d --name kafka --network kafka-net -p 9092:9092 `
-e CLUSTER_ID="5L6g3nShT-eMCtK--X86sw" `
-e KAFKA_NODE_ID="1" `
-e KAFKA_PROCESS_ROLES="broker,controller" `
-e KAFKA_CONTROLLER_QUORUM_VOTERS="1@kafka:9093" `
-e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" `
-e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://kafka:9092" `
-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" `
-e KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER" `
-e KAFKA_INTER_BROKER_LISTENER_NAME="PLAINTEXT" `
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1" `
-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="1" `
-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR="1" `
apache/kafka:latest
```
```
docker run -it --rm -p 8888:8888 --network kafka-net -v C:\Users\singh\Documents\aws-playground:/home/jovyan/work quay.io/jupyter/pyspark-notebook:latest
```

