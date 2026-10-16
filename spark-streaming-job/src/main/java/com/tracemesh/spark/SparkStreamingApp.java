package com.tracemesh.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

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
                // Ignore malformed JSON before it enters stateful aggregation.
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
                        expr("percentile_approx(executionDurationMs, 0.99)").cast(DataTypes.DoubleType).as("p99_latency_ms"),
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

        // Keep the Parquet layout flat so Hive's external table can read it
        // without partition discovery or metastore repair commands.
        StreamingQuery query = windowedMetrics.writeStream()
                .format("parquet")
                .outputMode("append")
                .option("path", outputPath)
                .option("checkpointLocation", checkpointPath)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        query.awaitTermination();
    }
}
