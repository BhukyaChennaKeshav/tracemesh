# TraceMesh

TraceMesh is a local proof-of-concept telemetry pipeline for microservice latency monitoring and SLA anomaly detection.

## Data flow

1. The Spring Boot producer publishes JSON trace spans to Kafka.
2. Spark Structured Streaming consumes telemetry and calculates 10-second event-time windows sliding every 5 seconds.
3. Spark pushes processed aggregates to the Spring Boot metrics API for the live dashboard.
4. Spark also persists the same processed windows as Parquet in HDFS; Hive exposes that directory for historical SQL analytics.

The dashboard reads only `GET /api/metrics`. It does not query Hive. The API keeps a bounded in-memory snapshot of recent windows, so live metrics reset when the API process restarts; historical Parquet data remains in HDFS.

## Stack

- Java 17 and Spring Boot for synthetic trace generation and the live metrics API
- Kafka in KRaft mode for ingestion
- Apache Spark 3.5 Structured Streaming for aggregation and live metric delivery
- HDFS and Hive 2.3 for Parquet persistence and historical SQL queries
- Docker Compose for local orchestration

This is a single-broker local PoC, not a production high-availability deployment. Kafka has one replica, and the sample data generator is synthetic.

## Run locally

Prerequisites: Docker Desktop / Docker Engine with the Compose plugin, and enough memory for Kafka, Spark, HDFS, and Hive (at least 8 GB allocated is a reasonable starting point).

Build and start the stack:

```bash
docker compose up --build -d
docker compose ps
```

The producer and Spark job require Kafka and HDFS, not Hive, to serve live metrics. Hive initialization runs independently for historical queries.

Open the dashboard at [http://localhost:8081](http://localhost:8081). Enable or disable the synthetic payment-service anomaly with the dashboard button. The dashboard only displays metrics actually processed by Spark; it does not generate fallback readings.

Check logs if the pipeline is still warming up:

```bash
docker compose logs -f kafka trace-producer spark-master spark-worker spark-streaming-app namenode datanode hive-server hive-init
```

The first streaming windows may take a little time to appear because Spark uses a 10-second processing trigger and event-time windows.

## Query the warehouse

Run the SLA query through HiveServer2:

```bash
docker compose exec hive-server beeline -u jdbc:hive2://localhost:10000/tracemesh_db -n hive -f /workspace/queries/01_detect_sla_violations.sql
```

Other example queries are in `hive-warehouse/queries`.

The sample SLA query flags windows where P99 latency exceeds 1000 ms. The anomaly mode injects 1500–3499 ms latency for payment-service spans; because the generator selects services randomly, only some spans are affected.

## Useful endpoints

- `GET /api/anomaly/status` — returns the current anomaly toggle state.
- `POST /api/anomaly/toggle?enable=true` — enables the synthetic payment-service degradation.
- `POST /api/metrics/ingest` — receives processed metric batches from Spark.
- `GET /api/metrics` — returns up to 50 latest processed metric windows from the API's in-memory cache.

## Configuration

The defaults are set for Docker Compose. Override them through the environment when needed:

- `KAFKA_BOOTSTRAP_SERVERS` (default `kafka:9092`)
- `KAFKA_TOPIC` (default `telemetry-spans`)
- `TRACE_INTERVAL_MS` (default `10`)
- `METRICS_API_URL` (default `http://trace-producer:8081/api/metrics/ingest`)
- `HDFS_OUTPUT_PATH` (default `hdfs://namenode:9000/user/hive/warehouse/tracemesh_metrics`)
- `HDFS_CHECKPOINT_PATH` (default `hdfs://namenode:9000/tmp/spark-checkpoints/tracemesh`)
- `SPARK_MASTER_URL` (default `spark://spark-master:7077`)
- `SPARK_DRIVER_HOST` (default `spark-streaming-app`)
- `SPARK_SQL_SHUFFLE_PARTITIONS` (default `3`)

## Troubleshooting

- If the dashboard has no metrics, inspect `trace-producer`, `spark-streaming-app`, Kafka, and HDFS logs. Hive is not in the live dashboard request path.
- Historical SQL queries still require HiveServer2 and the external table to be initialized.
- The producer and Spark consumer use the internal Compose DNS name `kafka:9092`. The advertised listener is designed for containers on the Compose network.
