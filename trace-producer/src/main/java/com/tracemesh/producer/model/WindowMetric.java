package com.tracemesh.producer.model;

/** One service's metrics for one sliding window, mirroring the columns the Spark job writes. */
public record WindowMetric(
        String serviceName,
        String windowStart,
        String windowEnd,
        double p99LatencyMs,
        double avgLatencyMs,
        long totalRequests,
        long errorCount,
        boolean slaBreached
) {}
