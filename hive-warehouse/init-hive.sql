CREATE DATABASE IF NOT EXISTS tracemesh_db;
USE tracemesh_db;

-- Spark writes Parquet under date_partition=YYYY-MM-DD/ subdirectories,
-- so the table has to be partitioned on that column for Hive to read it.
CREATE EXTERNAL TABLE IF NOT EXISTS service_latency_metrics (
    window_start STRING,
    window_end STRING,
    serviceName STRING,
    p99_latency_ms DOUBLE,
    avg_latency_ms DOUBLE,
    total_requests BIGINT,
    error_count BIGINT
)
PARTITIONED BY (date_partition STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/tracemesh_metrics';

-- Register partitions Spark has written so far (re-run to pick up new days).
-- The dashboard API runs this automatically before each warehouse query.
SET hive.msck.path.validation=ignore;
MSCK REPAIR TABLE service_latency_metrics;
