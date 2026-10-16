CREATE DATABASE IF NOT EXISTS tracemesh_db;
USE tracemesh_db;

-- Spark writes a flat Parquet directory. Keep this schema in the same
-- column order/names as SparkStreamingApp's output.
CREATE EXTERNAL TABLE IF NOT EXISTS service_latency_metrics (
    window_start STRING,
    window_end STRING,
    serviceName STRING,
    p99_latency_ms DOUBLE,
    avg_latency_ms DOUBLE,
    total_requests BIGINT,
    error_count BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/tracemesh_metrics';
