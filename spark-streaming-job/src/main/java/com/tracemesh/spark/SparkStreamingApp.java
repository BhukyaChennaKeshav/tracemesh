package com.tracemesh.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkStreamingApp {

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    public static void main(String[] args) throws Exception {
        // spark-submit supplies the master URL. Do not override it here, or the
        // job silently runs locally instead of using the configured Spark cluster.
        SparkSession spark = SparkSession.builder()
                .appName("TraceMesh-SparkStreaming")
                .config("spark.sql.shuffle.partitions",
                        env("SPARK_SQL_SHUFFLE_PARTITIONS", "3"))
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        String kafkaBootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String kafkaTopic = env("KAFKA_TOPIC", "telemetry-spans");
        String outputPath = env("HDFS_OUTPUT_PATH",
                "hdfs://namenode:9000/user/hive/warehouse/tracemesh_metrics");
        String checkpointPath = env("HDFS_CHECKPOINT_PATH",
                "hdfs://namenode:9000/tmp/spark-checkpoints/tracemesh");
        String metricsApiUrl = env("METRICS_API_URL",
                "http://trace-producer:8081/api/metrics/ingest");

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        StructType schema = new StructType()
                .add("serviceName", DataTypes.StringType)
                .add("traceId", DataTypes.StringType)
                .add("spanId", DataTypes.StringType)
                .add("timestamp", DataTypes.StringType)
                .add("executionDurationMs", DataTypes.LongType)
                .add("statusCode", DataTypes.IntegerType);

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", env("KAFKA_STARTING_OFFSETS", "latest"))
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> parsedStream = kafkaStream
                .selectExpr("CAST(value AS STRING) AS json_payload")
                .select(from_json(col("json_payload"), schema).as("data"))
                .select("data.*")
                .filter(col("serviceName").isNotNull()
                        .and(col("timestamp").isNotNull())
                        .and(col("executionDurationMs").isNotNull()))
                .withColumn("event_time", to_timestamp(col("timestamp")))
                .filter(col("event_time").isNotNull());

        Dataset<Row> windowedMetrics = parsedStream
                .withWatermark("event_time", "1 minute")
                .groupBy(
                        window(col("event_time"), "10 seconds", "5 seconds"),
                        col("serviceName")
                )
                .agg(
                        expr("percentile_approx(executionDurationMs, 0.99)")
                                .cast(DataTypes.DoubleType).as("p99_latency_ms"),
                        avg("executionDurationMs").as("avg_latency_ms"),
                        count("*").as("total_requests"),
                        sum(when(col("statusCode").geq(500), 1).otherwise(0)).as("error_count")
                )
                .select(
                        col("window.start").cast(DataTypes.StringType).as("window_start"),
                        col("window.end").cast(DataTypes.StringType).as("window_end"),
                        col("serviceName"),
                        col("p99_latency_ms"),
                        col("avg_latency_ms"),
                        col("total_requests"),
                        col("error_count")
                );

        // Send each completed aggregate batch to the live API and persist the same batch
        // to Parquet. API upserts are keyed by service/window, making Spark retries idempotent.
        StreamingQuery query = windowedMetrics.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointPath)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .foreachBatch((batch, batchId) -> {
                    List<String> jsonRows = batch.select(
                                    col("serviceName"),
                                    col("window_start").as("windowStart"),
                                    col("p99_latency_ms").as("p99LatencyMs"),
                                    col("avg_latency_ms").as("avgLatencyMs"),
                                    col("total_requests").as("totalRequests"),
                                    col("error_count").as("errorCount"))
                            .toJSON()
                            .collectAsList();

                    if (!jsonRows.isEmpty()) {
                        String payload = "[" + String.join(",", jsonRows) + "]";
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(metricsApiUrl))
                                .timeout(Duration.ofSeconds(15))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(payload))
                                .build();
                        HttpResponse<String> response = httpClient.send(
                                request, HttpResponse.BodyHandlers.ofString());

                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            throw new IllegalStateException("Metrics API returned HTTP "
                                    + response.statusCode() + ": " + response.body());
                        }
                        System.out.println("Pushed " + jsonRows.size()
                                + " processed metric rows to the API (batch " + batchId + ").");
                    }

                    // Preserve the historical analytics path; Hive reads these Parquet files.
                    batch.write().mode("append").parquet(outputPath);
                })
                .start();

        query.awaitTermination();
    }
}
