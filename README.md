# TraceMesh

TraceMesh is a local proof-of-concept telemetry pipeline for microservice latency monitoring and SLA anomaly detection.

## Data flow

1. The Spring Boot producer publishes JSON trace spans to Kafka.
2. Spark Structured Streaming consumes the topic, calculates 10-second event-time windows sliding every 5 seconds, and writes Parquet metrics to HDFS.
3. Hive exposes the Parquet directory through the `tracemesh_db.service_latency_metrics` external table.
4. The dashboard reads the latest stored windows from the producer's `/api/metrics` endpoint. If Hive is unavailable, the dashboard reports that state instead of inventing sample data.

## Stack

- Java 17 and Spring Boot for the synthetic trace producer and REST API
- Kafka in KRaft mode for ingestion
- Apache Spark 3.5 Structured Streaming for aggregation
- HDFS and Hive 2.3 for Parquet storage and SQL queries
- Docker Compose for local orchestration

This is a single-broker local PoC, not a production high-availability deployment. Kafka has one replica, and the sample data generator is synthetic.

## Run locally

Prerequisites: Docker Desktop / Docker Engine with the Compose plugin, and enough memory for Kafka, Spark, HDFS, and Hive (at least 8 GB allocated is a reasonable starting point).

Build and start the stack:

```bash
docker compose up --build -d
docker compose ps
```

Compose waits for Kafka, HDFS, PostgreSQL, and Hive readiness where supported. The one-shot `hive-init` service creates the database and external table before the producer and streaming job start.

Open the dashboard at [http://localhost:8081](http://localhost:8081). Enable or disable the synthetic payment-service anomaly with the dashboard button. The toggle controls the producer; the dashboard only displays metrics actually returned by the pipeline and does not simulate values when the metrics API fails.

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
- `GET /api/metrics` — returns up to 50 latest metric windows, or HTTP 503 when Hive cannot be queried.

## Configuration

The defaults are set for Docker Compose. Override them through the environment when needed:

- `KAFKA_BOOTSTRAP_SERVERS` (default `kafka:9092`)
- `KAFKA_TOPIC` (default `telemetry-spans`)
- `TRACE_INTERVAL_MS` (default `10`)
- `HIVE_JDBC_URL` (default `jdbc:hive2://hive-server:10000/tracemesh_db`)
- `HDFS_OUTPUT_PATH` (default `hdfs://namenode:9000/user/hive/warehouse/tracemesh_metrics`)
- `HDFS_CHECKPOINT_PATH` (default `hdfs://namenode:9000/tmp/spark-checkpoints/tracemesh`)
- `SPARK_MASTER_URL` (default `spark://spark-master:7077`)
- `SPARK_DRIVER_HOST` (default `spark-streaming-app`)
- `SPARK_SQL_SHUFFLE_PARTITIONS` (default `3`)

## Troubleshooting

- If the dashboard reports metrics unavailable, inspect `spark-streaming-app`, HDFS, and Hive logs. An empty response is not used to hide Hive errors.
- If you changed the external table schema or the Hive init SQL after an earlier run, recreate the local stack to re-run the one-shot initialization: `docker compose down -v` followed by `docker compose up --build -d`. **This removes local Kafka, HDFS, and Hive volume data.**
- The producer and Spark consumer use the internal Compose DNS name `kafka:9092`. The advertised listener is designed for containers on the Compose network.
