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

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("TraceMesh-SparkStreaming")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        StructType schema = new StructType()
                .add("serviceName", DataTypes.StringType)
                .add("traceId", DataTypes.StringType)
                .add("spanId", DataTypes.StringType)
                .add("timestamp", DataTypes.StringType)
                .add("executionDurationMs", DataTypes.LongType)
                .add("statusCode", DataTypes.IntegerType);

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "telemetry-spans")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false") // Prevents OffsetOutOfRangeException crashes
                .load();

        Dataset<Row> parsedStream = kafkaStream
                .selectExpr("CAST(value AS STRING) as json_payload")
                .select(from_json(col("json_payload"), schema).as("data"))
                .select("data.*")
                .withColumn("event_time", col("timestamp").cast(DataTypes.TimestampType));

        // Apply watermarking and sliding window aggregations
        Dataset<Row> windowedMetrics = parsedStream
                .withWatermark("event_time", "1 minute")
                .groupBy(
                        window(col("event_time"), "10 seconds", "5 seconds"),
                        col("serviceName")
                )
                .agg(
                        expr("percentile_approx(executionDurationMs, 0.99)").as("p99_latency_ms"),
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
                )
                .withColumn("date_partition", to_date(col("window_start")));

        // Write metrics stream directly to HDFS storage for Hive consumption
        StreamingQuery query = windowedMetrics.writeStream()
                .format("parquet")
                .outputMode("append")
                .partitionBy("date_partition")
                .option("path", "hdfs://namenode:9000/user/hive/warehouse/tracemesh_metrics")
                .option("checkpointLocation", "hdfs://namenode:9000/tmp/spark-checkpoints/tracemesh")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        query.awaitTermination();
    }
}