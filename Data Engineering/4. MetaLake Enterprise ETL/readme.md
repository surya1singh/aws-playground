You are building a production-grade enterprise data engineering platform named “AtlasFlow DataHub”.

Goal:
Build a metadata-driven ETL/ELT framework that can ingest from:
1) S3 text logs (*.log)
2) RDS Postgres (relational OLTP data)
3) Kafka/MSK event logs (JSON)

and transform into a governed Lakehouse on S3 using Apache Iceberg tables (bronze/silver/gold). The framework must be driven by mapping specs (YAML/JSON) and publish governance metadata + lineage to Apache Atlas via REST API.

Core requirements:
- Framework supports job types: batch snapshot, incremental (diff), streaming.
- Spec-driven transforms: select/rename/cast, rules, filters, joins, aggregations, SCD2 for dimensions.
- Spec-driven DQ checks: not_null, unique, accepted_values, range, freshness.
- Write run metadata (run_id/run_dt/status/rows_in/rows_out/dq_fail counts/duration/error) to a metadata table (Iceberg or Parquet).
- Orchestration: generate Airflow DAGs from specs OR provide Step Functions definition; include both minimal.
- Observability: store per-run metrics + emit alert hooks (SNS stub).
- Governance: implement an Atlas publisher module that:
  - registers datasets (Iceberg tables, S3 raw paths, Kafka topics),
  - attaches classifications (PII tags),
  - creates glossary terms (optional),
  - publishes lineage edges from inputs -> outputs based on spec DAG.

Deliverables:
1) Repository structure with modules:
   - spec loader + validator
   - source connectors: s3_logs, postgres_jdbc, kafka
   - spark engine: transformations + joins + aggregations
   - scd2 engine
   - dq engine
   - metadata logger
   - atlas publisher (REST client)
   - cli runner: run --spec --run_dt --run_id
   - airflow dag generator: generates dags from specs folder
2) Provide working local dev mode:
   - local Spark execution
   - local Kafka (docker compose)
   - local Postgres (docker compose)
   - local S3 simulated via filesystem paths
3) Provide integration mode for AWS:
   - EMR Spark submit OR Glue job entrypoint
   - MSK bootstrap config
   - RDS JDBC config
4) Provide unit tests:
   - rules tests
   - dq spec-driven tests
   - scd2 merge tests
   - contract tests for outputs

Also generate:
- Example specs:
  - logs_to_fact_api_requests.yaml (parse log lines -> structured table)
  - postgres_orders_to_dim_customer_scd2.yaml
  - kafka_order_events_to_fact_order_events_stream.yaml
- Example data models:
  - RDS: customers, addresses, products, merchants, orders, order_items, payments, shipments, returns, inventory, promotions, reviews
  - Kafka topics: clickstream_events, order_events, payment_events
  - Logs: api_gateway.log with request_id/user_id/latency/status/error_code/service

Finally:
- Write a README with architecture, how to run locally (docker compose + spark), and how to deploy to AWS.
- Implement raw data generators: logs, postgres seed, kafka producer (Python + Faker).
