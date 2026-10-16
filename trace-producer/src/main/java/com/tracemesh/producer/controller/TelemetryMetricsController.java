package com.tracemesh.producer.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class TelemetryMetricsController {

    private static final Logger log = LoggerFactory.getLogger(TelemetryMetricsController.class);

    @Value("${tracemesh.hive.jdbc-url}")
    private String hiveJdbcUrl;

    @GetMapping("/metrics")
    public ResponseEntity<?> getLatestMetrics() {
        List<Map<String, Object>> metrics = new ArrayList<>();
        String sql = "SELECT serviceName, window_start, p99_latency_ms, avg_latency_ms, " +
                "total_requests, error_count FROM service_latency_metrics " +
                "ORDER BY window_start DESC LIMIT 50";

        try (Connection conn = DriverManager.getConnection(hiveJdbcUrl, "hive", "");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("serviceName", rs.getString("serviceName"));
                row.put("windowStart", rs.getString("window_start"));
                row.put("p99LatencyMs", rs.getDouble("p99_latency_ms"));
                row.put("avgLatencyMs", rs.getDouble("avg_latency_ms"));
                row.put("totalRequests", rs.getLong("total_requests"));
                row.put("errorCount", rs.getLong("error_count"));
                metrics.add(row);
            }
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            // Do not disguise a broken pipeline as a valid empty result.
            log.warn("Unable to query TraceMesh metrics from Hive: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Metrics are unavailable. Check Hive and Spark service logs."));
        }
    }
}
